#!/usr/bin/env zx

import 'zx/globals';
import fsPromises from 'fs/promises';
import config from 'config';
import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import mime from 'mime-types';
import jsLogger from '@map-colonies/js-logger';
import { Tracing, logMethod } from '@map-colonies/telemetry';
import { trace as traceAPI, context as contextAPI, SpanStatusCode, SpanKind } from '@opentelemetry/api';
import { SemanticAttributes } from '@opentelemetry/semantic-conventions';
import { streamToString, getDiffDirPathComponents } from './util.mjs';
import {
  OSMDBT_GET_LOG,
  OSMDBT_CREATE_DIFF,
  OSMDBT_CATCHUP,
  STATE_FILE,
  OSMDBT_CONFIG_PATH,
  ExitCodes,
  S3_REGION,
  OSMDBT_BIN_PATH,
  OSMDBT_DONE_LOG_PREFIX,
  DIFF_FILE_EXTENTION,
} from './constants.mjs';

const tracing = new Tracing();
tracing.start();
const tracer = traceAPI.getTracer('osmdbt-wrapper');
const singleJobSpan = tracer.startSpan('single-job', { attributes: { 'job.rollback': false } });

class ErrorWithExitCode extends Error {
  constructor(message, exitCode) {
    super(message);
    this.exitCode = exitCode;
  }
}

const objectStorageConfig = config.get('objectStorage');
const osmdbtConfig = config.get('osmdbt');
const telemetryConfig = config.get('telemetry');
const OSMDBT_STATE_PATH = path.join(osmdbtConfig.changesDir, STATE_FILE);
const OSMDBT_STATE_BACKUP_PATH = path.join(osmdbtConfig.changesDir, 'backup', STATE_FILE);
const GLOBAL_OSMDBT_ARGS = osmdbtConfig.verbose ? ['-c', OSMDBT_CONFIG_PATH] : ['-c', OSMDBT_CONFIG_PATH, '-q'];
let baseS3SnapAttributes;
const baseOsmdbtSnapAttributes = {
  [SemanticAttributes.RPC_SYSTEM]: 'osmdbt',
};
const logger = jsLogger.default({ ...telemetryConfig.logger, hooks: { logMethod } });
let jobExitCode = ExitCodes.SUCCESS;
let filesUploaded = 0;
let s3Client;

const promisifySpan = async (spanName, spanAttributes, context, fn) => {
  return new Promise(async (resolve, reject) => {
    const span = tracer.startSpan(spanName, { attributes: spanAttributes }, context);
    try {
      const result = await fn();
      handleSpanOnSuccess(span);
      resolve(result);
    } catch (error) {
      handleSpanOnError(span, error);
      reject(error);
    }
  });
};

process.on('SIGINT', async () => {
  await processExitSafely(ExitCodes.TERMINATED);
});

const prepareEnvironment = async (span) => {
  const { logDir, changesDir, runDir } = osmdbtConfig;

  logger.debug({ msg: 'preparing environment', osmdbtDirs: { logDir, changesDir, runDir } });

  const backupDir = path.join(changesDir, 'backup');
  const uniqueDirs = [logDir, changesDir, runDir, backupDir].filter((dir, index, dirs) => dirs.indexOf(dir) === index);
  span.setAttribute('dir.create.count', uniqueDirs.length);

  const makeDirPromises = uniqueDirs.map(async (dir) => {
    logger.debug({ msg: 'creating directory', dir });
    await promisifySpan('fs.mkdir', { 'dir.path': dir }, contextAPI.active(), () => fsPromises.mkdir(dir, { recursive: true }));
  });

  try {
    await Promise.all(makeDirPromises);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }
  handleSpanOnSuccess(span);
};

const getSequenceNumber = async (span) => {
  logger.debug({ msg: 'fetching sequence number from file', file: OSMDBT_STATE_PATH });

  span.setAttribute('file.path', OSMDBT_STATE_PATH);
  const stateFileContent = await fsPromises.readFile(OSMDBT_STATE_PATH, 'utf-8');
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

const initializeS3Client = () => {
  const { endpoint, bucketName, acl } = objectStorageConfig;
  logger.info({ msg: 'initializing s3 client', endpoint, bucketName, acl });

  baseS3SnapAttributes = {
    [SemanticAttributes.RPC_SYSTEM]: 'aws.api',
    [SemanticAttributes.RPC_SERVICE]: 'S3',
    [SemanticAttributes.NET_TRANSPORT]: 'ip_tcp',
    [SemanticAttributes.NET_PEER_NAME]: endpoint,
    'aws.region': S3_REGION,
    's3.bucket.name': bucketName,
  };

  return new S3Client({
    signatureVersion: 'v4',
    endpoint,
    region: S3_REGION,
    forcePathStyle: true,
  });
};

const getStateFileFromS3ToFs = async (span) => {
  logger.debug({ msg: 'getting state file from s3' });
  let stateFileStream;

  try {
    stateFileStream = await getObjectWrapper(STATE_FILE);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }

  const stateFileContent = await streamToString(stateFileStream.Body);
  const writeFilesPromises = [OSMDBT_STATE_PATH, OSMDBT_STATE_BACKUP_PATH].map(async (path) => {
    logger.debug({ msg: 'writing file', path });
    await promisifySpan('fs.write', { 'file.path': path }, contextAPI.active(), () => fsPromises.writeFile(path, stateFileContent));
  });

  try {
    await Promise.all(writeFilesPromises);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }

  handleSpanOnSuccess(span);
};

const runOsmdbtCommand = async (command, commandArgs = [], span) => {
  const args = [...GLOBAL_OSMDBT_ARGS, ...commandArgs];
  logger.info({ msg: 'executing command', executable: 'osmdbt', command, args });

  try {
    const prettyArgs = args.join(' ');
    span.setAttributes({ ...baseOsmdbtSnapAttributes, 'osmdbt.command': command, 'osmdbt.command.args': `${prettyArgs}` });

    const commandPath = path.join(OSMDBT_BIN_PATH, command);
    await $`${commandPath} ${args}`;
    handleSpanOnSuccess(span);
  } catch (error) {
    logger.error({ msg: 'failure occurred during command execution', executable: 'osmdbt', command, args });

    handleSpanOnError(span, error);
    throw new ErrorWithExitCode(error.stderr, ExitCodes.OSMDBT_ERROR);
  }
};

const uploadDiff = async (sequenceNumber, span) => {
  const [top, bottom, stateNumber] = getDiffDirPathComponents(sequenceNumber);

  if (telemetryConfig.tracing.enabled) {
    const stateFilePath = path.join(osmdbtConfig.changesDir, top, bottom, `${stateNumber}.${STATE_FILE}`);
    const traceId = singleJobSpan.spanContext().traceId;
    await promisifySpan('fs.append', { 'file.path': stateFilePath }, contextAPI.active(), () =>
      fsPromises.appendFile(stateFilePath, `traceId=${traceId}`, 'utf-8')
    );
  }
  const newDiffAndStatePaths = [STATE_FILE, DIFF_FILE_EXTENTION].map((fileExtention) => path.join(top, bottom, `${stateNumber}.${fileExtention}`));

  logger.debug({ msg: 'uploading diff and state files', state: sequenceNumber, filesCount: newDiffAndStatePaths.length });

  const uploads = newDiffAndStatePaths.map(async (filePath) => {
    logger.debug({ msg: 'uploading file', filePath });

    const localPath = path.join(osmdbtConfig.changesDir, filePath);
    const uploadContent = await promisifySpan('fs.read', { 'file.path': localPath }, contextAPI.active(), () => fsPromises.readFile(localPath));
    await putObjectWrapper(filePath, uploadContent);
  });

  span.setAttributes({ 'upload.count': uploads.length, 'upload.state': sequenceNumber });

  try {
    await Promise.all(uploads);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }
  handleSpanOnSuccess(span);
};

const getObjectWrapper = async (key) => {
  let span;
  const bucketName = objectStorageConfig.bucketName;

  logger.debug({ msg: 'getting object from s3', bucketName, key });

  try {
    span = tracer.startSpan(
      's3.getObject',
      {
        kind: SpanKind.CLIENT,
        attributes: { ...baseS3SnapAttributes, [SemanticAttributes.RPC_METHOD]: 'GetObject', 's3.key': key },
      },
      contextAPI.active()
    );
    const stream = await s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: key }));
    handleSpanOnSuccess(span);
    return stream;
  } catch (error) {
    logger.error({ err: error, msg: 'failed getting key from bucket', bucketName, key });

    handleSpanOnError(span, error);

    throw new ErrorWithExitCode(
      `failed getting key: ${key} from bucket: ${bucketName} received the following error: ${error}`,
      ExitCodes.STATE_FETCH_FAILURE_ERROR
    );
  }
};

const putObjectWrapper = async (key, body) => {
  const { bucketName, acl } = objectStorageConfig;

  const possibleContentType = mime.contentType(key.split('/').pop());
  const contentType = possibleContentType ? possibleContentType : undefined;
  let span;

  logger.debug({ msg: 'putting key in bucket', key, bucketName, acl, contentType });

  try {
    span = tracer.startSpan(
      's3.putObject',
      {
        kind: SpanKind.CLIENT,
        attributes: {
          ...baseS3SnapAttributes,
          [SemanticAttributes.RPC_METHOD]: 'PutObject',
          's3.key': key,
          's3.content.type': contentType ?? 'unknown',
          's3.acl': acl,
        },
      },
      contextAPI.active()
    );

    await s3Client.send(new PutObjectCommand({ Bucket: bucketName, Key: key, Body: body, ContentType: contentType, ACL: acl }));
    handleSpanOnSuccess(span);
    filesUploaded++;
  } catch (error) {
    logger.error({ err: error, msg: 'failed putting key in bucket', acl, bucketName, key });

    handleSpanOnError(span, error);
    throw new ErrorWithExitCode(
      `failed putting key: ${key} into bucket: ${bucketName} received the following error: ${error}`,
      ExitCodes.PUT_OBJECT_ERROR
    );
  }
};

const commitChanges = async (span) => {
  logger.info({ msg: 'commiting changes by marking logs and catching up' });

  try {
    await tracer.startActiveSpan('mark-logs', undefined, contextAPI.active(), markLogFilesForCatchup);
    await tracer.startActiveSpan(
      'osmdbt.catchup',
      { kind: SpanKind.CLIENT },
      contextAPI.active(),
      async (span) => await runOsmdbtCommand(OSMDBT_CATCHUP, undefined, span)
    );
    handleSpanOnSuccess(span);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }
};

const markLogFilesForCatchup = async (span) => {
  const { logDir } = osmdbtConfig;
  const logFilesNames = await fsPromises.readdir(logDir);

  logger.debug({ msg: 'marking log files for catchup', count: logFilesNames.length });

  const renameFilesPromises = logFilesNames.map(async (logFileName) => {
    if (!logFileName.endsWith(OSMDBT_DONE_LOG_PREFIX)) {
      return;
    }
    const logFileNameForCatchup = logFileName.slice(0, logFileName.length - OSMDBT_DONE_LOG_PREFIX.length);
    const currentPath = path.join(logDir, logFileName);
    const newPath = path.join(logDir, logFileNameForCatchup);
    await promisifySpan('fs.rename', { 'file.path': currentPath, 'file.name': logFileName }, contextAPI.active(), () =>
      fsPromises.rename(currentPath, newPath)
    );
  });

  span.setAttribute('mark.count', renameFilesPromises.length);

  try {
    await Promise.all(renameFilesPromises);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }
  handleSpanOnSuccess(span);
};

const rollback = async (span) => {
  logger.info({ msg: 'something went wrong while processing state running rollback' });

  singleJobSpan.setAttribute('job.rollback', true);
  try {
    const backupState = await promisifySpan('fs.read', { 'file.path': OSMDBT_STATE_BACKUP_PATH, 'file.name': STATE_FILE }, contextAPI.active(), () =>
      fsPromises.readFile(OSMDBT_STATE_BACKUP_PATH)
    );
    await putObjectWrapper(STATE_FILE, backupState);
    handleSpanOnSuccess(span);
  } catch (error) {
    logger.error({ msg: 'failed to rollback', err: error });

    handleSpanOnError(span, error);
    await processExitSafely(ExitCodes.ROLLBACK_FAILURE_ERROR);
  }
};

const processExitSafely = async (exitCode = ExitCodes.GENERAL_ERROR) => {
  logger.info({ msg: 'exiting safely', exitCode });

  let rootSpanStatus;
  rootSpanStatus = exitCode == (ExitCodes.SUCCESS || ExitCodes.TERMINATED) ? SpanStatusCode.OK : SpanStatusCode.ERROR;
  singleJobSpan.setAttributes({ 'job.exitCode': exitCode, 'job.upload.count': filesUploaded });
  singleJobSpan.setStatus(rootSpanStatus);

  await cleanup();
  process.exit(exitCode);
};

const cleanup = async () => {
  logger.debug({ msg: 'cleaning up by stopping active processes' });
  if (s3Client !== undefined) {
    s3Client.destroy();
  }
  singleJobSpan.end();
  await tracing.stop();
};

const handleSpanOnSuccess = (span) => {
  span.setStatus(SpanStatusCode.OK);
  span.end();
};

const handleSpanOnError = (span, error) => {
  span.setStatus(SpanStatusCode.ERROR);
  const { exitCode, message, name, stack } = error;
  span.recordException({ code: exitCode, message, name, stack });
  span.end();
};

const main = async () => {
  logger.info({ msg: 'new job has started' });

  try {
    await tracer.startActiveSpan('prepare-environment', undefined, contextAPI.active(), prepareEnvironment);

    s3Client = initializeS3Client();

    await tracer.startActiveSpan('get-start-state', undefined, contextAPI.active(), getStateFileFromS3ToFs);
    const startState = await tracer.startActiveSpan('fs.read', undefined, contextAPI.active(), getSequenceNumber);

    logger.info({ msg: 'starting job with fetched start state from object storage', startState });

    singleJobSpan.setAttribute('job.state.start', startState);

    await tracer.startActiveSpan(
      'osmdbt.get-log',
      { kind: SpanKind.CLIENT },
      contextAPI.active(),
      async (span) => await runOsmdbtCommand(OSMDBT_GET_LOG, ['-m', osmdbtConfig.getLogMaxChanges], span)
    );

    await tracer.startActiveSpan(
      'osmdbt.create-diff',
      { kind: SpanKind.CLIENT },
      contextAPI.active(),
      async (span) => await runOsmdbtCommand(OSMDBT_CREATE_DIFF, undefined, span)
    );

    const newSequenceNumber = await tracer.startActiveSpan('fs.read', undefined, contextAPI.active(), getSequenceNumber);
    if (startState === newSequenceNumber) {
      logger.info({ msg: 'no diffs were found on this job, exiting gracefully', startState, endState });
      await processExitSafely(ExitCodes.SUCCESS);
    }

    logger.info({ msg: 'diff was created, starting the upload', state: newSequenceNumber });

    singleJobSpan.setAttribute('job.state.end', newSequenceNumber);

    await tracer.startActiveSpan('upload-diff', undefined, contextAPI.active(), async (span) => await uploadDiff(newSequenceNumber, span));

    logger.info({ msg: 'finished the upload of the diff, uploading state file', state: endState });

    const endState = await promisifySpan('fs.read', { 'file.path': OSMDBT_STATE_PATH, 'file.name': STATE_FILE }, contextAPI.active(), () =>
      fsPromises.readFile(OSMDBT_STATE_PATH)
    );
    await putObjectWrapper(STATE_FILE, endState);

    logger.info({ msg: 'finished the upload of the end state file, commiting changes', endState });

    try {
      await tracer.startActiveSpan('commit-changes', undefined, contextAPI.active(), commitChanges);
    } catch (error) {
      logger.error({ err: error, msg: 'an error accord during commiting changes for state', state: endState });

      await tracer.startActiveSpan('rollback', undefined, contextAPI.active(), rollback);
      singleJobSpan.setAttribute('job.state.end', startState);
      throw error;
    }

    logger.info({ msg: 'job completed successfully, exiting gracefully', startState, endState });
  } catch (error) {
    logger.error({ err: error, msg: 'an error occurred exiting safely', exitCode: error.exitCode });
    jobExitCode = error.exitCode;
  } finally {
    await processExitSafely(jobExitCode);
  }
};

const mainContext = traceAPI.setSpan(contextAPI.active(), singleJobSpan);
await contextAPI.with(mainContext, main);
