import { join } from 'path';
import { type Logger } from '@map-colonies/js-logger';
import { ActionStatus } from '@map-colonies/arstotzka-common';
import { Span, SpanStatusCode, type Tracer } from '@opentelemetry/api';
import { inject, injectable } from 'tsyringe';
import { handleSpanOnError, handleSpanOnSuccess } from '@map-colonies/telemetry';
import { Histogram, Counter as PromCounter, Registry as PromRegistry } from 'prom-client';
import { StatefulMediator } from '@map-colonies/arstotzka-mediator';
import { attemptSafely, extractSequenceNumber, getDiffDirPathComponents, streamToString } from '@src/common/util';
import { BACKUP_DIR_NAME, DIFF_FILE_EXTENTION, ExitCodes, OSMDBT_DONE_LOG_PREFIX, SERVICES, STATE_FILE } from '@src/common/constants';
import { ErrorWithExitCode } from '@src/common/errors';
import { JobAttributes, SpanName } from '@src/common/tracing/job';
import { type ConfigType } from '@src/common/config';
import { AppConfig, MetricsConfig, OsmdbtConfig } from '@src/common/interfaces';
import { S3Manager } from '@src/s3/s3Manager';
import { FsRepository } from '@src/fs/fsRepository';
import { S3Attributes } from '@src/common/tracing/s3';
import { promisifySpan } from '@src/common/tracing/util';
import { OsmdbtExecutable } from '@src/executables/osmdbt';
import { OsmiumExecutable } from '@src/executables/osmium';

@injectable()
export class OsmdbtService {
  private isActiveJob = false;

  private readonly appConfig: AppConfig;
  private readonly osmdbtConfig: OsmdbtConfig;

  private readonly jobCounter?: PromCounter;
  private readonly jobDurationHistogram?: Histogram;

  public constructor(
    @inject(SERVICES.LOGGER) private readonly logger: Logger,
    @inject(SERVICES.TRACER) private readonly tracer: Tracer,
    @inject(SERVICES.CONFIG) private readonly config: ConfigType,
    @inject(SERVICES.MEDIATOR) private readonly mediator: StatefulMediator,
    @inject(S3Manager) private readonly s3Manager: S3Manager,
    @inject(FsRepository) private readonly fsRepository: FsRepository,
    @inject(OsmdbtExecutable) private readonly osmdbtExecutable: OsmdbtExecutable,
    @inject(OsmiumExecutable) private readonly osmiumExecutable: OsmiumExecutable,
    @inject(SERVICES.METRICS) registry?: PromRegistry
  ) {
    this.appConfig = this.config.get('app') as AppConfig;
    this.osmdbtConfig = this.config.get('osmdbt') as OsmdbtConfig;

    if (registry !== undefined) {
      const { osmdbtCommandDurationSeconds } = (this.config.get('telemetry.metrics') as MetricsConfig).buckets;
      this.jobCounter = new PromCounter({
        name: 'osmdbt_job_count',
        help: 'The total number of osmdbt jobs started',
        registers: [registry],
      });
      this.jobDurationHistogram = new Histogram({
        name: 'osmdbt_job_duration_seconds',
        help: 'Duration of osmdbt job execution in seconds',
        labelNames: ['exitCode'] as const,
        buckets: osmdbtCommandDurationSeconds,
        registers: [registry],
      });
    }
  }

  private get osmdbtStatePath(): string {
    return join(this.osmdbtConfig.changesDir, STATE_FILE);
  }

  private get osmdbtStateBackupPath(): string {
    return join(this.osmdbtConfig.changesDir, BACKUP_DIR_NAME, STATE_FILE);
  }

  public async executeJob(): Promise<void> {
    if (this.isActiveJob) {
      this.logger.warn({ msg: 'job is already active, skipping execution' });
      return;
    }

    this.isActiveJob = true;
    let jobExitCode = ExitCodes.SUCCESS;

    const jobTimer = this.jobDurationHistogram?.startTimer();
    this.jobCounter?.inc();

    await this.tracer.startActiveSpan(SpanName.ROOT_JOB, { attributes: { [JobAttributes.JOB_ROLLBACK]: false } }, async (rootJobSpan) => {
      try {
        await this.startJob(rootJobSpan);
      } catch (error) {
        this.logger.error({ msg: 'an error occurred durion job execution, exiting safely', err: error });
        jobExitCode = error instanceof ErrorWithExitCode ? error.exitCode : ExitCodes.GENERAL_ERROR;
        throw error;
      } finally {
        jobTimer?.({ exitCode: jobExitCode.toString() });
        rootJobSpan.setAttributes({ [JobAttributes.JOB_EXITCODE]: jobExitCode });
        rootJobSpan.setStatus({ code: jobExitCode === (ExitCodes.SUCCESS || ExitCodes.TERMINATED) ? SpanStatusCode.OK : SpanStatusCode.ERROR });
        rootJobSpan.end();
        this.isActiveJob = false;
      }
    });
  }

  private async startJob(span?: Span): Promise<void> {
    this.logger.info({ msg: 'new job has started' });

    await this.mediator.reserveAccess();

    await this.tracer.startActiveSpan(SpanName.PREPARE_ENVIRONMENT, async (span) => promisifySpan(async () => this.prepareEnvironment(), span));

    await this.tracer.startActiveSpan(SpanName.PULL_STATE_FILE, async (span) => promisifySpan(async () => this.pullStateFile(), span));

    const startState = await this.getSequenceNumber();

    this.logger.info({ msg: 'starting job with fetched start state from object storage', startState });

    span?.setAttribute(JobAttributes.JOB_STATE_START, startState);

    await this.osmdbtExecutable.getLog();

    await this.osmdbtExecutable.createDiff();

    const endState = await this.getSequenceNumber();

    span?.setAttribute(JobAttributes.JOB_STATE_END, endState);

    if (startState === endState) {
      this.logger.info({ msg: 'no diffs were found on this job, exiting gracefully', startState, endState });

      await attemptSafely(async () => this.mediator.removeLock());
      return;
    }

    await this.mediator.createAction({ state: +endState });

    await attemptSafely(async () => this.mediator.removeLock());

    this.logger.info({ msg: 'diff was created, starting the upload of end state diff', startState, endState });

    await this.tracer.startActiveSpan(SpanName.UPLOAD_DIFF, async (span) => this.uploadDiff(endState, span));

    this.logger.info({ msg: 'finished the upload of the end state file, commiting changes', startState, endState });

    await this.tracer.startActiveSpan(SpanName.COMMIT_CHANGES, async (span) => {
      try {
        await this.commitChanges(span);
      } catch (commitError) {
        this.logger.warn({ msg: 'something went wrong while processing state, running rollback' });
        await this.tracer.startActiveSpan(SpanName.ROLLBACK, async (span) => {
          try {
            span.setAttribute(JobAttributes.JOB_ROLLBACK, true);
            await this.rollback(span);
          } catch (rollbackError) {
            await attemptSafely(async () => this.mediator.updateAction({ status: ActionStatus.FAILED, metadata: { error: rollbackError } }));
            throw rollbackError;
          }
        });
        span.setAttribute(JobAttributes.JOB_STATE_END, startState);
        await attemptSafely(async () => this.mediator.updateAction({ status: ActionStatus.FAILED, metadata: { error: commitError } }));
        throw commitError;
      }
    });

    await this.tracer.startActiveSpan(SpanName.POST_CATCHUP, async (span) => this.postCatchupCleanup(span));

    const metadata: Record<string, unknown> = {};
    await attemptSafely(async () => {
      metadata.info = await this.collectInfo(endState);
    });

    await this.mediator.updateAction({ status: ActionStatus.COMPLETED, metadata });

    this.logger.info({ msg: 'job completed successfully, exiting gracefully', startState, endState });
  }

  private async prepareEnvironment(): Promise<void> {
    const { logDir, changesDir, runDir } = this.osmdbtConfig;
    this.logger.debug({ msg: 'preparing environment', osmdbtDirs: { logDir, changesDir, runDir } });

    const backupDir = join(changesDir, BACKUP_DIR_NAME);
    const uniqueDirs = [logDir, changesDir, runDir, backupDir].filter((dir, index, dirs) => dirs.indexOf(dir) === index);

    const makeDirPromises = uniqueDirs.map(async (dir) => {
      this.logger.debug({ msg: 'creating directory', dir });
      await this.fsRepository.mkdir(dir);
    });

    await Promise.all(makeDirPromises);
  }

  private async getSequenceNumber(): Promise<string> {
    this.logger.debug({ msg: 'fetching sequence number from file', file: this.osmdbtStatePath });

    const stateFileContent = (await this.fsRepository.readFile(this.osmdbtStatePath, 'utf-8')) as string;

    try {
      return extractSequenceNumber(stateFileContent);
    } catch (err) {
      this.logger.error({ msg: 'failed to fetch sequence number from file', file: this.osmdbtStatePath, err });

      const error = new ErrorWithExitCode(
        `failed to fetch sequence number out of the state file, ${STATE_FILE} is invalid`,
        ExitCodes.INVALID_STATE_FILE_ERROR
      );

      throw error;
    }
  }

  private async uploadDiff(sequenceNumber: string, span?: Span): Promise<void> {
    const [top, bottom, stateNumber] = getDiffDirPathComponents(sequenceNumber);

    const newDiffAndStatePaths = [STATE_FILE, DIFF_FILE_EXTENTION].map((fileExtention) => join(top, bottom, `${stateNumber}.${fileExtention}`));

    this.logger.debug({ msg: 'uploading diff and state files', state: sequenceNumber, filesCount: newDiffAndStatePaths.length });

    const uploads = newDiffAndStatePaths.map(async (filePath) => {
      this.logger.debug({ msg: 'uploading file', filePath });

      const localPath = join(this.osmdbtConfig.changesDir, filePath);
      const uploadContent = (await this.fsRepository.readFile(localPath)) as Buffer;

      await this.s3Manager.putObject(filePath, uploadContent);
    });

    span?.setAttributes({ [S3Attributes.S3_UPLOAD_COUNT]: uploads.length, [S3Attributes.S3_UPLOAD_STATE]: sequenceNumber });

    try {
      await Promise.all(uploads);
      const endStateFileBuffer = (await this.fsRepository.readFile(this.osmdbtStatePath)) as Buffer;

      await this.s3Manager.putObject(STATE_FILE, endStateFileBuffer);
      handleSpanOnSuccess(span);
    } catch (error) {
      await attemptSafely(async () => this.mediator.updateAction({ status: ActionStatus.FAILED, metadata: { error } }));
      handleSpanOnError(span, error);
      throw error;
    }
  }

  private async commitChanges(span?: Span): Promise<void> {
    this.logger.info({ msg: 'commiting changes by marking logs and catching up' });

    try {
      await this.tracer.startActiveSpan(SpanName.MARK_LOGS, async (span) => this.markLogFilesForCatchup(span));
      await this.osmdbtExecutable.catchup();
      handleSpanOnSuccess(span);
    } catch (error) {
      this.logger.error({
        err: error,
        msg: 'an error accord during commiting changes for end state',
      });
      handleSpanOnError(span, error);
      throw error;
    }
  }

  private async postCatchupCleanup(span?: Span): Promise<void> {
    const { logDir } = this.osmdbtConfig;
    try {
      const logFilesNames = await this.fsRepository.readdir(logDir);

      this.logger.info({ msg: 'post catchup cleanup, log files to unlink', count: logFilesNames.length });

      const unlinkFilesPromises = logFilesNames.map(async (logFileName) => {
        const logFilePath = join(logDir, logFileName);
        return this.fsRepository.unlink(logFilePath);
      });

      span?.setAttribute('unlink.count', unlinkFilesPromises.length);

      await Promise.all(unlinkFilesPromises);
    } catch (error) {
      await attemptSafely(async () => this.mediator.updateAction({ status: ActionStatus.FAILED, metadata: { error } }));
      handleSpanOnError(span, error);
      throw error;
    }
    handleSpanOnSuccess(span);
  }

  private async markLogFilesForCatchup(span?: Span): Promise<void> {
    const { logDir } = this.osmdbtConfig;
    try {
      const logFilesNames = await this.fsRepository.readdir(logDir);

      this.logger.debug({ msg: 'marking log files for catchup', count: logFilesNames.length });

      const renameFilesPromises = logFilesNames.map(async (logFileName) => {
        if (!logFileName.endsWith(OSMDBT_DONE_LOG_PREFIX)) {
          return;
        }
        const logFileNameForCatchup = logFileName.slice(0, logFileName.length - OSMDBT_DONE_LOG_PREFIX.length);
        const currentPath = join(logDir, logFileName);
        const newPath = join(logDir, logFileNameForCatchup);
        await this.fsRepository.rename(currentPath, newPath);
      });

      span?.setAttribute('mark.count', renameFilesPromises.length);

      await Promise.all(renameFilesPromises);
      handleSpanOnSuccess(span);
    } catch (error) {
      handleSpanOnError(span, error);
      throw error;
    }
  }

  private async rollback(span?: Span): Promise<void> {
    this.logger.warn({ msg: 'attempting to rollback state from backup' });

    try {
      const backupStateFileBuffer = (await this.fsRepository.readFile(this.osmdbtStateBackupPath)) as Buffer;
      await this.s3Manager.putObject(STATE_FILE, backupStateFileBuffer);
      handleSpanOnSuccess(span);
    } catch (error) {
      this.logger.fatal({ msg: 'failed to rollback', err: error });
      handleSpanOnError(span, error);
      throw new ErrorWithExitCode('rollback error', ExitCodes.ROLLBACK_FAILURE_ERROR);
    }
  }

  private async collectInfo(sequenceNumber: string): Promise<Record<string, unknown>> {
    const [top, bottom, stateNumber] = getDiffDirPathComponents(sequenceNumber);
    const localdiffPath = join(this.osmdbtConfig.changesDir, top, bottom, `${stateNumber}.${DIFF_FILE_EXTENTION}`);
    const info = await this.osmiumExecutable.fileInfo(localdiffPath);
    return JSON.parse(info) as Record<string, unknown>;
  }

  private async pullStateFile(): Promise<void> {
    const stateFileStream = await this.s3Manager.getObject(STATE_FILE);

    const stateFileContent = await streamToString(stateFileStream);
    const writeFilesPromises = [this.osmdbtStatePath, this.osmdbtStateBackupPath].map(async (filePath) => {
      await this.fsRepository.writeFile(filePath, stateFileContent);
    });

    await Promise.all(writeFilesPromises);
  }
}
