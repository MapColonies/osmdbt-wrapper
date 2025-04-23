import { join } from 'path';
import { readFile, readdir, rename, appendFile, writeFile, mkdir } from 'fs/promises';
import config from 'config';
import { ActionStatus } from '@map-colonies/arstotzka-common';
import { StatefulMediator } from '@map-colonies/arstotzka-mediator';
import { Tracing } from '@map-colonies/telemetry';
import { trace as traceAPI, context as contextAPI, SpanStatusCode, SpanKind, SpanStatus, Span } from '@opentelemetry/api';
import { SemanticAttributes } from '@opentelemetry/semantic-conventions';
import execa from 'execa';
import cron from 'node-cron';
import { ObjectCannedACL } from '@aws-sdk/client-s3';
import {
  BACKUP_DIR_NAME,
  DIFF_FILE_EXTENTION,
  Executable,
  ExitCodes,
  OsmdbtCommand,
  OSMDBT_BIN_PATH,
  OSMDBT_CONFIG_PATH,
  OSMDBT_DONE_LOG_PREFIX,
  S3_LOCK_FILE_NAME,
  STATE_FILE,
} from './constants';
import { logger } from './telemetry/logger';
import { OsmdbtConfig, ObjectStorageConfig, TracingConfig, ArstotzkaConfig, AppConfig, OsmiumConfig } from './interfaces';
import { ErrorWithExitCode } from './errors';
import { getDiffDirPathComponents, streamToString } from './util';
import { FsSpanName, FsAttributes } from './telemetry/tracing/fs';
import { ExecutableAttributes, CommandSpanName } from './telemetry/tracing/executable';
import { JobAttributes, ROOT_JOB_SPAN_NAME } from './telemetry/tracing/job';
import { handleSpanOnSuccess, handleSpanOnError, promisifySpan, TRACER_NAME } from './telemetry/tracing/util';
import { deleteObjectWrapper, getObjectWrapper, headObjectWrapper, putObjectWrapper } from './s3';

const tracing = new Tracing();
tracing.start();
const tracer = traceAPI.getTracer(TRACER_NAME);
const rootJobSpan = tracer.startSpan(ROOT_JOB_SPAN_NAME, { attributes: { [JobAttributes.JOB_ROLLBACK]: false } });

const objectStorageConfig = config.get<ObjectStorageConfig>('objectStorage');
const tracingConfig = config.get<TracingConfig>('telemetry.tracing');
const osmdbtConfig = config.get<OsmdbtConfig>('osmdbt');
const osmiumConfig = config.get<OsmiumConfig>('osmium');
const arstotzkaConfig = config.get<ArstotzkaConfig>('arstotzka');
const appConfig = config.get<AppConfig>('app');
const cronConfig = config.get<AppConfig['cron']>('app.cron');

const OSMDBT_STATE_PATH = join(osmdbtConfig.changesDir, STATE_FILE);
const OSMDBT_STATE_BACKUP_PATH = join(osmdbtConfig.changesDir, BACKUP_DIR_NAME, STATE_FILE);
const GLOBAL_OSMDBT_ARGS = osmdbtConfig.verbose ? ['-c', OSMDBT_CONFIG_PATH] : ['-c', OSMDBT_CONFIG_PATH, '-q'];
const GLOBAL_OSMIUM_ARGS = osmiumConfig.verbose
  ? ['--verbose', osmiumConfig.progress ? '--progress' : '--no-progress']
  : [osmiumConfig.progress ? '--progress' : '--no-progress'];
const MILLISECONDS_IN_SECOND = 1000;

let jobExitCode = ExitCodes.SUCCESS;
let isS3Locked = false;
let filesUploaded = 0;
let mediator: StatefulMediator | undefined;
let cronJob: cron.ScheduledTask | undefined;
let shouldRun = true;

if (arstotzkaConfig.enabled) {
  mediator = new StatefulMediator({ ...arstotzkaConfig.mediator, serviceId: arstotzkaConfig.serviceId, logger });
}

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
    shouldRun = false;
    cronJob?.stop();
    cleanup(ExitCodes.TERMINATED).finally(() => process.exit(ExitCodes.TERMINATED));
  });
}

const prepareEnvironment = async (span?: Span): Promise<void> => {
  const { logDir, changesDir, runDir } = osmdbtConfig;

  logger.debug({ msg: 'preparing environment', osmdbtDirs: { logDir, changesDir, runDir } });

  const backupDir = join(changesDir, BACKUP_DIR_NAME);
  const uniqueDirs = [logDir, changesDir, runDir, backupDir].filter((dir, index, dirs) => dirs.indexOf(dir) === index);
  span?.setAttribute(FsAttributes.DIR_MK_COUNT, uniqueDirs.length);

  const makeDirPromises = uniqueDirs.map(async (dir) => {
    logger.debug({ msg: 'creating directory', dir });
    await promisifySpan(FsSpanName.FS_MKDIR, { [FsAttributes.DIR_PATH]: dir }, contextAPI.active(), async () => mkdir(dir, { recursive: true }));
  });

  try {
    await Promise.all(makeDirPromises);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }
  handleSpanOnSuccess(span);
};

const getSequenceNumber = async (span?: Span): Promise<string> => {
  logger.debug({ msg: 'fetching sequence number from file', file: OSMDBT_STATE_PATH });

  span?.setAttribute(FsAttributes.FILE_PATH, OSMDBT_STATE_PATH);

  const stateFileContent = await readFile(OSMDBT_STATE_PATH, 'utf-8');
  const matchResult = stateFileContent.match(/sequenceNumber=\d+/);
  if (matchResult === null || matchResult.length === 0) {
    logger.error({ msg: 'failed to fetch sequence number from file', file: OSMDBT_STATE_PATH });

    const error = new ErrorWithExitCode(
      `failed to fetch sequence number out of the state file, ${STATE_FILE} is invalid`,
      ExitCodes.INVALID_STATE_FILE_ERROR
    );
    handleSpanOnError(span, error);
    throw error;
  }

  const sequenceNumber = matchResult[0].split('=')[1];
  handleSpanOnSuccess(span);
  return sequenceNumber;
};

const lockS3 = async (span?: Span): Promise<void> => {
  const bucketName = objectStorageConfig.bucketName;

  logger.info({ msg: 'locking s3 bucket', bucketName, lockFileName: S3_LOCK_FILE_NAME });

  try {
    const headObjectResponse = await headObjectWrapper(objectStorageConfig.bucketName, S3_LOCK_FILE_NAME);

    if (headObjectResponse !== undefined) {
      logger.error({ msg: 's3 bucket is locked', bucketName, lockFileName: S3_LOCK_FILE_NAME });
      const error = new ErrorWithExitCode('s3 bucket is locked', ExitCodes.S3_LOCKED_ERROR);
      throw error;
    }

    const lockfileBuffer = Buffer.alloc(1, 0);

    await putObjectWrapper(objectStorageConfig.bucketName, S3_LOCK_FILE_NAME, lockfileBuffer, ObjectCannedACL.public_read);
    filesUploaded++;
    isS3Locked = true;
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }

  handleSpanOnSuccess(span);
};

const unlockS3 = async (span?: Span): Promise<void> => {
  const bucketName = objectStorageConfig.bucketName;

  logger.info({ msg: 'unlocking s3 bucket', bucketName, lockFileName: S3_LOCK_FILE_NAME });

  try {
    await deleteObjectWrapper(objectStorageConfig.bucketName, S3_LOCK_FILE_NAME);
    isS3Locked = false;
  } catch (error) {
    handleSpanOnError(span, error);
    jobExitCode = ExitCodes.S3_ERROR;
    logger.fatal({ err: error, msg: 'failed to unlock s3, unlock it manually', lockFileName: S3_LOCK_FILE_NAME, exitCode: jobExitCode });
    return;
  }

  handleSpanOnSuccess(span);
};

const getStateFileFromS3ToFs = async (span?: Span): Promise<void> => {
  logger.debug({ msg: 'getting state file from s3' });
  let stateFileStream;

  try {
    stateFileStream = await getObjectWrapper(objectStorageConfig.bucketName, STATE_FILE);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }

  const stateFileContent = await streamToString(stateFileStream);
  const writeFilesPromises = [OSMDBT_STATE_PATH, OSMDBT_STATE_BACKUP_PATH].map(async (path) => {
    logger.debug({ msg: 'writing file', path });
    await promisifySpan(FsSpanName.FS_WRITE, { [FsAttributes.FILE_PATH]: path }, contextAPI.active(), async () => writeFile(path, stateFileContent));
  });

  try {
    await Promise.all(writeFilesPromises);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }

  handleSpanOnSuccess(span);
};

const runCommand = async (executable: Executable, command: string, commandArgs: string[] = [], span?: Span): Promise<string> => {
  const executablePath = executable === 'osmdbt' ? join(OSMDBT_BIN_PATH, command) : executable;
  const args = executable === 'osmdbt' ? commandArgs : [command, ...commandArgs];

  logger.info({ msg: 'executing command', executable, command, args });

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
    logger.error({ msg: 'failure occurred during command execution', executable: 'osmdbt', command, args });

    handleSpanOnError(span, error);
    const exitCode = executable === 'osmdbt' ? ExitCodes.OSMDBT_ERROR : ExitCodes.OSMIUM_ERROR;
    if (error instanceof Error) {
      throw new ErrorWithExitCode(error.message, exitCode);
    }

    throw new ErrorWithExitCode(`${executable} errored`, exitCode);
  }
};

const collectInfo = async (sequenceNumber: string): Promise<Record<string, unknown>> => {
  const [top, bottom, stateNumber] = getDiffDirPathComponents(sequenceNumber);
  const localdiffPath = join(osmdbtConfig.changesDir, top, bottom, `${stateNumber}.${DIFF_FILE_EXTENTION}`);

  const collectedInfo = await tracer.startActiveSpan(CommandSpanName.FILE_INFO, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
    runCommand('osmium', 'fileinfo', [...GLOBAL_OSMIUM_ARGS, '--extended', '--json', localdiffPath], span)
  );

  return JSON.parse(collectedInfo) as Record<string, unknown>;
};

const uploadDiff = async (sequenceNumber: string, span?: Span): Promise<void> => {
  const [top, bottom, stateNumber] = getDiffDirPathComponents(sequenceNumber);

  if (tracingConfig.enabled) {
    const stateFilePath = join(osmdbtConfig.changesDir, top, bottom, `${stateNumber}.${STATE_FILE}`);
    const traceId = rootJobSpan.spanContext().traceId;
    await promisifySpan(FsSpanName.FS_APPEND, { [FsAttributes.FILE_PATH]: stateFilePath }, contextAPI.active(), async () =>
      appendFile(stateFilePath, `traceId=${traceId}`, 'utf-8')
    );
  }
  const newDiffAndStatePaths = [STATE_FILE, DIFF_FILE_EXTENTION].map((fileExtention) => join(top, bottom, `${stateNumber}.${fileExtention}`));

  logger.debug({ msg: 'uploading diff and state files', state: sequenceNumber, filesCount: newDiffAndStatePaths.length });

  const uploads = newDiffAndStatePaths.map(async (filePath) => {
    logger.debug({ msg: 'uploading file', filePath });

    const localPath = join(osmdbtConfig.changesDir, filePath);
    const uploadContent = await promisifySpan(FsSpanName.FS_READ, { [FsAttributes.FILE_PATH]: localPath }, contextAPI.active(), async () =>
      readFile(localPath)
    );
    await putObjectWrapper(objectStorageConfig.bucketName, filePath, uploadContent, objectStorageConfig.acl);
    filesUploaded++;
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
};

const commitChanges = async (span?: Span): Promise<void> => {
  logger.info({ msg: 'commiting changes by marking logs and catching up' });

  try {
    await tracer.startActiveSpan('mark-logs', {}, contextAPI.active(), markLogFilesForCatchup);
    await tracer.startActiveSpan(CommandSpanName.CATCHUP, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
      runCommand('osmdbt', OsmdbtCommand.CATCHUP, GLOBAL_OSMDBT_ARGS, span)
    );
    handleSpanOnSuccess(span);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }
};

const markLogFilesForCatchup = async (span?: Span): Promise<void> => {
  const { logDir } = osmdbtConfig;
  const logFilesNames = await readdir(logDir);

  logger.debug({ msg: 'marking log files for catchup', count: logFilesNames.length });

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
};

const rollback = async (span?: Span): Promise<number | undefined> => {
  logger.info({ msg: 'something went wrong while processing state running rollback' });

  rootJobSpan.setAttribute(JobAttributes.JOB_ROLLBACK, true);

  try {
    const backupStateFileBuffer = await promisifySpan(
      FsSpanName.FS_READ,
      { [FsAttributes.FILE_PATH]: OSMDBT_STATE_BACKUP_PATH, [FsAttributes.FILE_NAME]: STATE_FILE },
      contextAPI.active(),
      async () => readFile(OSMDBT_STATE_BACKUP_PATH)
    );
    await putObjectWrapper(objectStorageConfig.bucketName, STATE_FILE, backupStateFileBuffer, objectStorageConfig.acl);
    filesUploaded++;
    handleSpanOnSuccess(span);
  } catch (error) {
    logger.fatal({ msg: 'failed to rollback, for safety reasons keeping the s3 bucket locked, unlock it manually', err: error });

    handleSpanOnError(span, error);
    return await processExitSafely(ExitCodes.ROLLBACK_FAILURE_ERROR);
  }
};

const processExitSafely = async (exitCode = ExitCodes.GENERAL_ERROR): Promise<number> => {
  logger.info({ msg: 'exiting safely', exitCode });

  await cleanup(exitCode);

  return exitCode;
};

const cleanup = async (exitCode: number): Promise<void> => {
  logger.debug({ msg: 'cleaning up by stopping active processes' });

  const rootSpanStatus: SpanStatus = { code: SpanStatusCode.UNSET };
  rootSpanStatus.code = exitCode == (ExitCodes.SUCCESS || ExitCodes.TERMINATED) ? SpanStatusCode.OK : SpanStatusCode.ERROR;
  rootJobSpan.setAttributes({ [JobAttributes.JOB_EXITCODE]: exitCode, [JobAttributes.JOB_UPLOAD_COUNT]: filesUploaded });

  rootJobSpan.setStatus(rootSpanStatus);
  rootJobSpan.end();

  await tracing.stop();
};

const main = async (): Promise<number> => {
  logger.info({ msg: 'new job has started' });

  try {
    await mediator?.reserveAccess();

    await tracer.startActiveSpan('prepare-environment', {}, contextAPI.active(), prepareEnvironment);

    if (appConfig.shouldLockObjectStorage) {
      await tracer.startActiveSpan('lock-s3', {}, contextAPI.active(), lockS3);
    }

    await tracer.startActiveSpan('get-start-state', {}, contextAPI.active(), getStateFileFromS3ToFs);
    const startState = await tracer.startActiveSpan(FsSpanName.FS_READ, {}, contextAPI.active(), getSequenceNumber);

    logger.info({ msg: 'starting job with fetched start state from object storage', startState });

    rootJobSpan.setAttribute(JobAttributes.JOB_STATE_START, startState);

    await tracer.startActiveSpan(CommandSpanName.GET_LOG, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
      runCommand('osmdbt', OsmdbtCommand.GET_LOG, [...GLOBAL_OSMDBT_ARGS, '-m', osmdbtConfig.getLogMaxChanges.toString()], span)
    );

    await tracer.startActiveSpan(CommandSpanName.CREATE_DIFF, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
      runCommand('osmdbt', OsmdbtCommand.CREATE_DIFF, GLOBAL_OSMDBT_ARGS, span)
    );

    const endState = await tracer.startActiveSpan(FsSpanName.FS_READ, {}, contextAPI.active(), getSequenceNumber);

    rootJobSpan.setAttribute(JobAttributes.JOB_STATE_END, endState);

    if (startState === endState) {
      logger.info({ msg: 'no diffs were found on this job, exiting gracefully', startState, endState });

      await mediator?.removeLock();

      if (appConfig.shouldLockObjectStorage) {
        await tracer.startActiveSpan('unlock-s3', {}, contextAPI.active(), unlockS3);
      }

      return await processExitSafely(ExitCodes.SUCCESS);
    }

    await mediator?.createAction({ state: +endState });

    await mediator?.removeLock();

    logger.info({ msg: 'diff was created, starting the upload of end state diff', startState, endState });

    await tracer.startActiveSpan('upload-diff', {}, contextAPI.active(), async (span) => uploadDiff(endState, span));

    logger.info({ msg: 'finished the upload of the diff, uploading end state file', startState, endState });

    const endStateFileBuffer = await promisifySpan(
      FsSpanName.FS_READ,
      { [FsAttributes.FILE_PATH]: OSMDBT_STATE_PATH, [FsAttributes.FILE_NAME]: STATE_FILE },
      contextAPI.active(),
      async () => readFile(OSMDBT_STATE_PATH)
    );
    await putObjectWrapper(objectStorageConfig.bucketName, STATE_FILE, endStateFileBuffer, objectStorageConfig.acl);

    filesUploaded++;

    logger.info({ msg: 'finished the upload of the end state file, commiting changes', startState, endState });

    try {
      await tracer.startActiveSpan('commit-changes', {}, contextAPI.active(), commitChanges);
    } catch (error) {
      logger.error({ err: error, msg: 'an error accord during commiting changes for end state, rollbacking to start state', startState, endState });

      await tracer.startActiveSpan('rollback', {}, contextAPI.active(), rollback);
      rootJobSpan.setAttribute(JobAttributes.JOB_STATE_END, startState);
      throw error;
    }

    const metadata: Record<string, unknown> = {};
    if (appConfig.shouldCollectInfo) {
      metadata.info = await collectInfo(endState);
    }

    await mediator?.updateAction({ status: ActionStatus.COMPLETED, metadata });

    logger.info({ msg: 'job completed successfully, exiting gracefully', startState, endState });
  } catch (error) {
    logger.error({ err: error, msg: 'an error occurred exiting safely' });
    if (error instanceof ErrorWithExitCode) {
      jobExitCode = error.exitCode;
    } else {
      jobExitCode = ExitCodes.GENERAL_ERROR;
    }

    await mediator?.updateAction({ status: ActionStatus.FAILED, metadata: { error } });
  } finally {
    if (appConfig.shouldLockObjectStorage && isS3Locked) {
      await tracer.startActiveSpan('unlock-s3', {}, contextAPI.active(), unlockS3);
    }
    await processExitSafely(jobExitCode);
  }
  return jobExitCode;
};

const init = async (): Promise<void> => {
  const mainContext = traceAPI.setSpan(contextAPI.active(), rootJobSpan);
  await contextAPI.with(mainContext, main);
};

if (cronConfig?.enabled && cron.validate(cronConfig.expression)) {
  logger.info({ msg: 'running in cron job mode', cronConfig });
  cronJob = cron.schedule(cronConfig.expression, () => {
    if (!shouldRun) {
      return;
    }
    void new Promise((resolve) => {
      shouldRun = false;
      init()
        .then(resolve)
        .catch((error: unknown) => {
          logger.error({ msg: 'an error occurred during cron job execution. strting penalty timer', timeout: cronConfig.failurePenalty, err: error });

          setTimeout(() => {
            logger.info({ msg: 'penalty timer finished, cron job will run again' });
          }, cronConfig.failurePenalty * MILLISECONDS_IN_SECOND);

          resolve(error);
        })
        .finally(() => (shouldRun = true));
    });
  });
} else {
  logger.info({ msg: 'running in one time mode' });
  void init();
}
