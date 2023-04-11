import { join } from 'path';
import { readFile, readdir, rename, appendFile, writeFile, mkdir } from 'fs/promises';
import config from 'config';
import { ActionStatus } from '@map-colonies/arstotzka-common';
import { StatefulMediator } from '@map-colonies/arstotzka-mediator';
import { Tracing } from '@map-colonies/telemetry';
import { trace as traceAPI, context as contextAPI, SpanStatusCode, SpanKind, SpanStatus, Span } from '@opentelemetry/api';
import { SemanticAttributes } from '@opentelemetry/semantic-conventions';
import execa from 'execa';
import { ObjectCannedACL } from '@aws-sdk/client-s3';
import {
  BACKUP_DIR_NAME,
  DIFF_FILE_EXTENTION,
  ExitCodes,
  OsmdbtCommand,
  OSMDBT_BIN_PATH,
  OSMDBT_CONFIG_PATH,
  OSMDBT_DONE_LOG_PREFIX,
  S3_LOCK_FILE_NAME,
  STATE_FILE,
} from './constants';
import { logger } from './telemetry/logger';
import { OsmdbtConfig, ObjectStorageConfig, TracingConfig, ArstotzkaConfig } from './interfaces';
import { ErrorWithExitCode } from './errors';
import { getDiffDirPathComponents, streamToString } from './util';
import { FsSpanName, FsAttributes } from './telemetry/tracing/fs';
import { OsmdbtAttributes, OsmdbtSpanName } from './telemetry/tracing/osmdbt';
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
const arstotzkaConfig = config.get<ArstotzkaConfig>('arstotzka');

const OSMDBT_STATE_PATH = join(osmdbtConfig.changesDir, STATE_FILE);
const OSMDBT_STATE_BACKUP_PATH = join(osmdbtConfig.changesDir, BACKUP_DIR_NAME, STATE_FILE);
const GLOBAL_OSMDBT_ARGS = osmdbtConfig.verbose ? ['-c', OSMDBT_CONFIG_PATH] : ['-c', OSMDBT_CONFIG_PATH, '-q'];

let jobExitCode = ExitCodes.SUCCESS;
let isS3Locked = false;
let filesUploaded = 0;
let mediator: StatefulMediator | undefined;

if (arstotzkaConfig.enabled) {
  mediator = new StatefulMediator({ ...arstotzkaConfig.mediator, serviceId: arstotzkaConfig.serviceId, logger });
}

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
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

const runOsmdbtCommand = async (command: string, commandArgs: string[] = [], span?: Span): Promise<void> => {
  const args = [...GLOBAL_OSMDBT_ARGS, ...commandArgs];
  logger.info({ msg: 'executing command', executable: 'osmdbt', command, args });

  try {
    span?.setAttributes({
      [SemanticAttributes.RPC_SYSTEM]: 'osmdbt',
      [OsmdbtAttributes.OSMDBT_COMMAND]: command,
      [OsmdbtAttributes.OSMDBT_COMMAND_ARGS]: args.join(' '),
    });

    const commandPath = join(OSMDBT_BIN_PATH, command);
    const spawnedChild = execa(commandPath, args, { encoding: 'utf-8' });
    const { exitCode, stderr } = await spawnedChild;

    if (exitCode !== 0) {
      throw new ErrorWithExitCode(stderr.length > 0 ? stderr : `osmdbt ${command} failed with exit code ${exitCode}`, ExitCodes.OSMDBT_ERROR);
    }

    handleSpanOnSuccess(span);
  } catch (error) {
    logger.error({ msg: 'failure occurred during command execution', executable: 'osmdbt', command, args });

    handleSpanOnError(span, error);
    if (error instanceof Error) {
      throw new ErrorWithExitCode(error.message, ExitCodes.OSMDBT_ERROR);
    }

    throw new ErrorWithExitCode('osmdbt errored', ExitCodes.OSMDBT_ERROR);
  }
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
    await putObjectWrapper(objectStorageConfig.bucketName, filePath, uploadContent);
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
    await tracer.startActiveSpan(OsmdbtSpanName.CATCHUP, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
      runOsmdbtCommand(OsmdbtCommand.CATCHUP, undefined, span)
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

const rollback = async (span?: Span): Promise<void> => {
  logger.info({ msg: 'something went wrong while processing state running rollback' });

  rootJobSpan.setAttribute(JobAttributes.JOB_ROLLBACK, true);

  try {
    const backupStateFileBuffer = await promisifySpan(
      FsSpanName.FS_READ,
      { [FsAttributes.FILE_PATH]: OSMDBT_STATE_BACKUP_PATH, [FsAttributes.FILE_NAME]: STATE_FILE },
      contextAPI.active(),
      async () => readFile(OSMDBT_STATE_BACKUP_PATH)
    );
    await putObjectWrapper(objectStorageConfig.bucketName, STATE_FILE, backupStateFileBuffer);
    filesUploaded++;
    handleSpanOnSuccess(span);
  } catch (error) {
    logger.fatal({ msg: 'failed to rollback, for safety reasons keeping the s3 bucket locked, unlock it manually', err: error });

    handleSpanOnError(span, error);
    await processExitSafely(ExitCodes.ROLLBACK_FAILURE_ERROR);
  }
};

const processExitSafely = async (exitCode = ExitCodes.GENERAL_ERROR): Promise<void> => {
  logger.info({ msg: 'exiting safely', exitCode });

  await cleanup(exitCode);

  process.exit(exitCode);
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

const main = async (): Promise<void> => {
  logger.info({ msg: 'new job has started' });

  try {
    await mediator?.reserveAccess();

    await tracer.startActiveSpan('prepare-environment', {}, contextAPI.active(), prepareEnvironment);

    await tracer.startActiveSpan('lock-s3', {}, contextAPI.active(), lockS3);

    await tracer.startActiveSpan('get-start-state', {}, contextAPI.active(), getStateFileFromS3ToFs);
    const startState = await tracer.startActiveSpan(FsSpanName.FS_READ, {}, contextAPI.active(), getSequenceNumber);

    logger.info({ msg: 'starting job with fetched start state from object storage', startState });

    rootJobSpan.setAttribute(JobAttributes.JOB_STATE_START, startState);

    await tracer.startActiveSpan(OsmdbtSpanName.GET_LOG, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
      runOsmdbtCommand(OsmdbtCommand.GET_LOG, ['-m', osmdbtConfig.getLogMaxChanges.toString()], span)
    );

    await tracer.startActiveSpan(OsmdbtSpanName.CREATE_DIFF, { kind: SpanKind.CLIENT }, contextAPI.active(), async (span) =>
      runOsmdbtCommand(OsmdbtCommand.CREATE_DIFF, undefined, span)
    );

    const endState = await tracer.startActiveSpan(FsSpanName.FS_READ, {}, contextAPI.active(), getSequenceNumber);

    rootJobSpan.setAttribute(JobAttributes.JOB_STATE_END, endState);

    if (startState === endState) {
      logger.info({ msg: 'no diffs were found on this job, exiting gracefully', startState, endState });

      await mediator?.removeLock();

      await tracer.startActiveSpan('unlock-s3', {}, contextAPI.active(), unlockS3);

      await processExitSafely(ExitCodes.SUCCESS);
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
    await putObjectWrapper(objectStorageConfig.bucketName, STATE_FILE, endStateFileBuffer);
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

    await mediator?.updateAction({ status: ActionStatus.COMPLETED });

    logger.info({ msg: 'job completed successfully, exiting gracefully', startState, endState });
  } catch (error) {
    logger.error({ err: error, msg: 'an error occurred exiting safely' });
    if (error instanceof ErrorWithExitCode) {
      jobExitCode = error.exitCode;
    }

    await mediator?.updateAction({ status: ActionStatus.FAILED, metadata: { error } });
  } finally {
    if (isS3Locked) {
      await tracer.startActiveSpan('unlock-s3', {}, contextAPI.active(), unlockS3);
    }

    await processExitSafely(jobExitCode);
  }
};

const mainContext = traceAPI.setSpan(contextAPI.active(), rootJobSpan);
void contextAPI.with(mainContext, main);
