import { readFile, mkdir, appendFile, readdir, rename, unlink } from 'fs/promises';
import { join } from 'path';
import execa from 'execa';
import { type Logger } from '@map-colonies/js-logger';
import { ActionStatus } from '@map-colonies/arstotzka-common';
import { Span, SpanKind, SpanStatus, SpanStatusCode, context as contextAPI, type Tracer } from '@opentelemetry/api';
import { inject, injectable, singleton } from 'tsyringe';
import { handleSpanOnError, handleSpanOnSuccess } from '@map-colonies/telemetry';
import { SemanticAttributes } from '@opentelemetry/semantic-conventions';
import {
  BACKUP_DIR_NAME,
  DIFF_FILE_EXTENTION,
  Executable,
  ExitCodes,
  OSMDBT_BIN_PATH,
  OSMDBT_CONFIG_PATH,
  OSMDBT_DONE_LOG_PREFIX,
  OsmdbtCommand,
  SERVICES,
  STATE_FILE,
} from '@src/common/constants';
import { ErrorWithExitCode } from '@src/common/errors';
import { JobAttributes, ROOT_JOB_SPAN_NAME } from '@src/common/tracing/job';
import { type Mediator } from '@src/mediator';
import { type ConfigType } from '@src/common/config';
import { AppConfig, OsmdbtConfig, OsmiumConfig } from '@src/common/interfaces';
import { promisifySpan } from '@src/common/tracing/util';
import { FsAttributes, FsSpanName } from '@src/common/tracing/fs';
import { S3Manager } from '@src/s3/s3Manager';
import { CommandSpanName, ExecutableAttributes } from '@src/common/tracing/executable';
import { getDiffDirPathComponents } from '@src/util';
import { tryCatch } from '@src/try-catch';

@singleton()
@injectable()
export class OsmdbtService {
  private static isActiveJob = false;
  private rootJobSpan: Span | undefined;
  private readonly appConfig: AppConfig;
  private readonly osmdbtConfig: OsmdbtConfig;
  private readonly globalOsmdbtArgs: string[];
  private readonly osmdbtStatePath: string;
  private readonly osmdbtStateBackupPath: string;

  public constructor(
    @inject(SERVICES.LOGGER) private readonly logger: Logger,
    @inject(SERVICES.TRACER) private readonly tracer: Tracer,
    @inject(SERVICES.CONFIG) private readonly config: ConfigType,
    @inject(SERVICES.MEDIATOR) private readonly mediator: Mediator,
    @inject(S3Manager) private readonly s3Manager: S3Manager
  ) {
    this.appConfig = this.config.get('app') as AppConfig;
    this.osmdbtConfig = this.config.get('osmdbt') as OsmdbtConfig;
    this.globalOsmdbtArgs = this.osmdbtConfig.verbose ? ['-c', OSMDBT_CONFIG_PATH] : ['-c', OSMDBT_CONFIG_PATH, '-q'];
    this.osmdbtStatePath = join(this.osmdbtConfig.changesDir, STATE_FILE);
    this.osmdbtStateBackupPath = join(this.osmdbtConfig.changesDir, BACKUP_DIR_NAME, STATE_FILE);
  }

  public static isJobActive(): boolean {
    return OsmdbtService.isActiveJob;
  }
  public async startJob(): Promise<void> {
    let jobExitCode: (typeof ExitCodes)[keyof typeof ExitCodes] = ExitCodes.SUCCESS;

    if (OsmdbtService.isActiveJob) {
      this.logger.warn({ msg: 'job is already active, skipping the start', currentRootJobSpan: this.rootJobSpan });
      return;
    }
    OsmdbtService.isActiveJob = true;

    this.logger.info({ msg: 'new job has started' });
    this.rootJobSpan = this.tracer.startSpan(ROOT_JOB_SPAN_NAME, { attributes: { [JobAttributes.JOB_ROLLBACK]: false } });

    try {
      await this.mediator.reserveAccess();

      await this.tracer.startActiveSpan('prepare-environment', {}, contextAPI.active(), this.prepareEnvironment.bind(this));

      await this.tracer.startActiveSpan('get-start-state', {}, contextAPI.active(), async (span) => {
        return this.s3Manager.getStateFileFromS3ToFs(
          {
            path: this.osmdbtStatePath,
            backupPath: this.osmdbtStateBackupPath,
          },
          span
        );
      });
      const startState = await this.tracer.startActiveSpan(FsSpanName.FS_READ, {}, contextAPI.active(), this.getSequenceNumber.bind(this));

      this.logger.info({ msg: 'starting job with fetched start state from object storage', startState });

      this.rootJobSpan.setAttribute(JobAttributes.JOB_STATE_START, startState);

      await this.tracer.startActiveSpan(CommandSpanName.GET_LOG, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
        this.runCommand('osmdbt', OsmdbtCommand.GET_LOG, [...this.globalOsmdbtArgs, '-m', this.osmdbtConfig.getLogMaxChanges.toString()], span)
      );

      await this.tracer.startActiveSpan(CommandSpanName.CREATE_DIFF, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
        this.runCommand('osmdbt', OsmdbtCommand.CREATE_DIFF, this.globalOsmdbtArgs, span)
      );

      const endState = await this.tracer.startActiveSpan(FsSpanName.FS_READ, {}, contextAPI.active(), this.getSequenceNumber.bind(this));

      this.rootJobSpan.setAttribute(JobAttributes.JOB_STATE_END, endState);

      if (startState === endState) {
        this.logger.info({ msg: 'no diffs were found on this job, exiting gracefully', startState, endState });

        await tryCatch(this.mediator.removeLock());

        return this.processExitSafely(ExitCodes.SUCCESS);
      }

      await tryCatch(this.mediator.createAction({ state: +endState }));

      await tryCatch(this.mediator.removeLock());

      this.logger.info({ msg: 'diff was created, starting the upload of end state diff', startState, endState });

      await this.tracer.startActiveSpan('upload-diff', {}, contextAPI.active(), async (span) => this.uploadDiff(endState, span));

      this.logger.info({ msg: 'finished the upload of the diff, uploading end state file', startState, endState });

      const endStateFileBuffer = await promisifySpan(
        FsSpanName.FS_READ,
        { [FsAttributes.FILE_PATH]: this.osmdbtStatePath, [FsAttributes.FILE_NAME]: STATE_FILE },
        contextAPI.active(),
        async () => readFile(this.osmdbtStatePath)
      );
      await this.s3Manager.uploadFile(STATE_FILE, endStateFileBuffer);

      this.logger.info({ msg: 'finished the upload of the end state file, commiting changes', startState, endState });

      const commitResult = await this.tracer.startActiveSpan('commit-changes', {}, contextAPI.active(), async (span) =>
        tryCatch(this.commitChanges(span))
      );
      if (commitResult.error) {
        this.logger.error({
          err: commitResult.error,
          msg: 'an error accord during commiting changes for end state, rollbacking to start state',
          startState,
          endState,
        });

        const rollbakcResponse = await this.tracer.startActiveSpan('rollback', {}, contextAPI.active(), async (span) =>
          tryCatch(this.rollback(span))
        );
        if (rollbakcResponse.error) {
          return this.processExitSafely(ExitCodes.ROLLBACK_FAILURE_ERROR);
        }
        this.rootJobSpan.setAttribute(JobAttributes.JOB_STATE_END, startState);
        throw commitResult.error;
      }

      const metadata: Record<string, unknown> = {};
      if (this.appConfig.shouldCollectInfo) {
        metadata.info = await this.collectInfo(endState);
      }

      await tryCatch(this.mediator.updateAction({ status: ActionStatus.COMPLETED, metadata }));

      this.logger.info({ msg: 'job completed successfully, exiting gracefully', startState, endState });
    } catch (error) {
      this.logger.error({ err: error, msg: 'an error occurred exiting safely' });
      if (error instanceof ErrorWithExitCode) {
        jobExitCode = error.exitCode;
      } else {
        jobExitCode = ExitCodes.GENERAL_ERROR;
      }

      await tryCatch(this.mediator.updateAction({ status: ActionStatus.FAILED, metadata: { error } }));
    } finally {
      this.processExitSafely(jobExitCode);
    }
  }

  private async prepareEnvironment(span?: Span): Promise<void> {
    const { logDir, changesDir, runDir } = this.osmdbtConfig;
    this.logger.debug({ msg: 'preparing environment', osmdbtDirs: { logDir, changesDir, runDir } });

    const backupDir = join(changesDir, BACKUP_DIR_NAME);
    const uniqueDirs = [logDir, changesDir, runDir, backupDir].filter((dir, index, dirs) => dirs.indexOf(dir) === index);
    span?.setAttribute(FsAttributes.DIR_MK_COUNT, uniqueDirs.length);

    const makeDirPromises = uniqueDirs.map(async (dir) => {
      this.logger.debug({ msg: 'creating directory', dir });
      await promisifySpan(FsSpanName.FS_MKDIR, { [FsAttributes.DIR_PATH]: dir }, contextAPI.active(), async () => mkdir(dir, { recursive: true }));
    });

    try {
      await Promise.all(makeDirPromises);
    } catch (error) {
      handleSpanOnError(span, error);
      throw error;
    }
    handleSpanOnSuccess(span);
  }

  private async runCommand(executable: Executable, command: string, commandArgs: string[] = [], span?: Span): Promise<string> {
    const executablePath = executable === 'osmdbt' ? join(OSMDBT_BIN_PATH, command) : executable;
    const args = executable === 'osmdbt' ? commandArgs : [command, ...commandArgs];

    this.logger.info({ msg: 'executing command', executable, command, args });

    span?.setAttributes({
      [SemanticAttributes.RPC_SYSTEM]: executable,
      [ExecutableAttributes.EXECUTABLE_COMMAND]: command,
      [ExecutableAttributes.EXECUTABLE_COMMAND_ARGS]: args.join(' '),
    });

    try {
      const spawnedChild = execa(executablePath, args, { encoding: 'utf-8' });

      const { exitCode, stderr, stdout } = await spawnedChild;

      if (exitCode !== 0) {
        throw new ErrorWithExitCode(stderr.length > 0 ? stderr : `osmdbt ${command} failed with exit code ${exitCode}`, ExitCodes.OSMDBT_ERROR);
      }

      handleSpanOnSuccess(span);

      return stdout;
    } catch (error) {
      this.logger.error({ msg: 'failure occurred during command execution', executable: 'osmdbt', command, args });

      handleSpanOnError(span, error);
      const exitCode = executable === 'osmdbt' ? ExitCodes.OSMDBT_ERROR : ExitCodes.OSMIUM_ERROR;
      if (error instanceof Error) {
        throw new ErrorWithExitCode(error.message, exitCode);
      }

      throw new ErrorWithExitCode(`${executable} errored`, exitCode);
    }
  }

  private async getSequenceNumber(span?: Span): Promise<string> {
    this.logger.debug({ msg: 'fetching sequence number from file', file: this.osmdbtStatePath });

    span?.setAttribute(FsAttributes.FILE_PATH, this.osmdbtStatePath);

    const stateFileContent = await readFile(this.osmdbtStatePath, 'utf-8');
    const matchResult = stateFileContent.match(/sequenceNumber=\d+/);
    if (matchResult === null || matchResult.length === 0) {
      this.logger.error({ msg: 'failed to fetch sequence number from file', file: this.osmdbtStatePath });

      const error = new ErrorWithExitCode(
        `failed to fetch sequence number out of the state file, ${STATE_FILE} is invalid`,
        ExitCodes.INVALID_STATE_FILE_ERROR
      );
      handleSpanOnError(span, error);
      throw error;
    }

    const sequenceNumber = matchResult[0].split('=')[1];
    if (typeof sequenceNumber != 'string') {
      throw new ErrorWithExitCode(`sequenceNumber is not a string, got ${typeof sequenceNumber}`, ExitCodes.INVALID_STATE_FILE_ERROR);
    }
    handleSpanOnSuccess(span);
    return sequenceNumber;
  }

  private processExitSafely(exitCode: number): void {
    this.logger.info({ msg: 'exiting safely', exitCode });

    const rootSpanStatus: SpanStatus = { code: SpanStatusCode.UNSET };
    rootSpanStatus.code = exitCode == (ExitCodes.SUCCESS || ExitCodes.TERMINATED) ? SpanStatusCode.OK : SpanStatusCode.ERROR;

    this.rootJobSpan?.setAttributes({ [JobAttributes.JOB_EXITCODE]: exitCode });

    this.rootJobSpan?.setStatus(rootSpanStatus);
    this.rootJobSpan?.end();

    OsmdbtService.isActiveJob = false;
  }

  private async uploadDiff(sequenceNumber: string, span?: Span): Promise<void> {
    const [top, bottom, stateNumber] = getDiffDirPathComponents(sequenceNumber);

    if (this.config.get('telemetry.tracing').isEnabled) {
      const stateFilePath = join(this.osmdbtConfig.changesDir, top, bottom, `${stateNumber}.${STATE_FILE}`);
      const traceId = this.rootJobSpan?.spanContext().traceId;
      await promisifySpan(FsSpanName.FS_APPEND, { [FsAttributes.FILE_PATH]: stateFilePath }, contextAPI.active(), async () =>
        appendFile(stateFilePath, `traceId=${traceId}`, 'utf-8')
      );
    }
    const newDiffAndStatePaths = [STATE_FILE, DIFF_FILE_EXTENTION].map((fileExtention) => join(top, bottom, `${stateNumber}.${fileExtention}`));

    this.logger.debug({ msg: 'uploading diff and state files', state: sequenceNumber, filesCount: newDiffAndStatePaths.length });

    const uploads = newDiffAndStatePaths.map(async (filePath) => {
      this.logger.debug({ msg: 'uploading file', filePath });

      const localPath = join(this.osmdbtConfig.changesDir, filePath);
      const uploadContent = await promisifySpan(FsSpanName.FS_READ, { [FsAttributes.FILE_PATH]: localPath }, contextAPI.active(), async () =>
        readFile(localPath)
      );
      await this.s3Manager.uploadFile(filePath, uploadContent);
    });

    // eslint-disable-next-line @typescript-eslint/naming-convention
    span?.setAttributes({ 'upload.count': uploads.length, 'upload.state': sequenceNumber });

    try {
      await Promise.all(uploads);
    } catch (error) {
      handleSpanOnError(span, error);
      throw error;
    }
    handleSpanOnSuccess(span);
  }

  private async commitChanges(span?: Span): Promise<void> {
    this.logger.info({ msg: 'commiting changes by marking logs and catching up' });

    try {
      await this.tracer.startActiveSpan('mark-logs', {}, contextAPI.active(), this.markLogFilesForCatchup.bind(this));
      await this.tracer.startActiveSpan(CommandSpanName.CATCHUP, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
        this.runCommand('osmdbt', OsmdbtCommand.CATCHUP, this.globalOsmdbtArgs, span)
      );
      await this.tracer.startActiveSpan(`post-${CommandSpanName.CATCHUP}`, {}, contextAPI.active(), this.postCatchupCleanup.bind(this));

      handleSpanOnSuccess(span);
    } catch (error) {
      handleSpanOnError(span, error);
      throw error;
    }
  }

  private async postCatchupCleanup(span?: Span): Promise<void> {
    const { logDir } = this.osmdbtConfig;
    try {
      const logFilesNames = await readdir(logDir);
      this.logger.info({ msg: 'unlinking log files', count: logFilesNames.length });

      const unlinkFilesPromises = logFilesNames.map(async (logFileName) => {
        const logFilePath = join(logDir, logFileName);
        this.logger.debug({ msg: 'unlinking file', filePath: logFilePath });
        return promisifySpan(
          FsSpanName.FS_UNLINK,
          { [FsAttributes.FILE_PATH]: logFilePath, [FsAttributes.FILE_NAME]: logFileName },
          contextAPI.active(),
          async () => unlink(logFilePath)
        );
      });

      span?.setAttribute('unlink.count', unlinkFilesPromises.length);

      await Promise.all(unlinkFilesPromises);
    } catch (error) {
      handleSpanOnError(span, error);
      throw error;
    }
    handleSpanOnSuccess(span);
  }

  private async markLogFilesForCatchup(span?: Span): Promise<void> {
    const { logDir } = this.osmdbtConfig;
    const logFilesNames = await readdir(logDir);

    this.logger.debug({ msg: 'marking log files for catchup', count: logFilesNames.length });

    const renameFilesPromises = logFilesNames.map(async (logFileName) => {
      if (!logFileName.endsWith(OSMDBT_DONE_LOG_PREFIX)) {
        return;
      }
      const logFileNameForCatchup = logFileName.slice(0, logFileName.length - OSMDBT_DONE_LOG_PREFIX.length);
      const currentPath = join(logDir, logFileName);
      const newPath = join(logDir, logFileNameForCatchup);
      await promisifySpan(
        FsSpanName.FS_RENAME,
        { [FsAttributes.FILE_PATH]: currentPath, [FsAttributes.FILE_NAME]: logFileName },
        contextAPI.active(),
        async () => rename(currentPath, newPath)
      );
    });

    span?.setAttribute('mark.count', renameFilesPromises.length);

    try {
      await Promise.all(renameFilesPromises);
    } catch (error) {
      handleSpanOnError(span, error);
      throw error;
    }
    handleSpanOnSuccess(span);
  }

  private async rollback(span?: Span): Promise<void> {
    this.logger.warn({ msg: 'something went wrong while processing state running rollback' });

    this.rootJobSpan?.setAttribute(JobAttributes.JOB_ROLLBACK, true);

    try {
      const backupStateFileBuffer = await promisifySpan(
        FsSpanName.FS_READ,
        { [FsAttributes.FILE_PATH]: this.osmdbtStateBackupPath, [FsAttributes.FILE_NAME]: STATE_FILE },
        contextAPI.active(),
        async () => readFile(this.osmdbtStateBackupPath)
      );
      await this.s3Manager.uploadFile(STATE_FILE, backupStateFileBuffer, span);
      handleSpanOnSuccess(span);
    } catch (error) {
      this.logger.fatal({ msg: 'failed to rollback', err: error });
      handleSpanOnError(span, error);
      throw error;
    }
  }

  private async collectInfo(sequenceNumber: string): Promise<Record<string, unknown>> {
    const osmiumConfig = this.config.get('osmium') as OsmiumConfig;
    const GLOBAL_OSMIUM_ARGS = osmiumConfig.verbose
      ? ['--verbose', osmiumConfig.progress ? '--progress' : '--no-progress']
      : [osmiumConfig.progress ? '--progress' : '--no-progress'];
    const [top, bottom, stateNumber] = getDiffDirPathComponents(sequenceNumber);
    const localdiffPath = join(this.osmdbtConfig.changesDir, top, bottom, `${stateNumber}.${DIFF_FILE_EXTENTION}`);

    const collectedInfo = await this.tracer.startActiveSpan(CommandSpanName.FILE_INFO, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
      this.runCommand('osmium', 'fileinfo', [...GLOBAL_OSMIUM_ARGS, '--extended', '--json', localdiffPath], span)
    );

    return JSON.parse(collectedInfo) as Record<string, unknown>;
  }
}
