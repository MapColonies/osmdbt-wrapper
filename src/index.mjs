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
  logger.debug(`preparing environment`);
  const { logDir, changesDir, runDir } = osmdbtConfig;
  const backupDir = path.join(changesDir, 'backup');
  const uniqueDirs = [logDir, changesDir, runDir, backupDir].filter((dir, index, dirs) => dirs.indexOf(dir) === index);
  span.setAttribute('dir.create.amount', uniqueDirs.length);

  const makeDirPromises = uniqueDirs.map(async (dir) => {
    logger.debug(`creating directory ${dir}`);
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
  logger.debug(`getting sequenceNumber from ${OSMDBT_STATE_PATH}`);
  span.setAttribute('file.path', OSMDBT_STATE_PATH);
  const stateFileContent = await fsPromises.readFile(OSMDBT_STATE_PATH, 'utf-8');
  const matchResult = stateFileContent.match(/sequenceNumber=\d+/);
  if (matchResult === null || matchResult.length === 0) {
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
  const { endpoint, bucketName } = objectStorageConfig;
  logger.debug(`initializing s3 client, configured endpoint: ${endpoint}, bucket: ${bucketName}`);

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
  logger.debug(`getting state file from s3`);
  let stateFileStream;

  try {
    stateFileStream = await getObjectWrapper(STATE_FILE);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }

  const stateFileContent = await streamToString(stateFileStream.Body);
  const writeFilesPromises = [OSMDBT_STATE_PATH, OSMDBT_STATE_BACKUP_PATH].map(async (path) => {
    logger.debug(`writing file ${path}`);
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
  logger.info(`preparing run of ${command}`);
  try {
    const args = [...GLOBAL_OSMDBT_ARGS, ...commandArgs];
    const commandPath = path.join(OSMDBT_BIN_PATH, command);
    const prettyArgs = args.join(' ');
    logger.debug(`command to be run: ${commandPath} ${prettyArgs}`);
    span.setAttributes({ ...baseOsmdbtSnapAttributes, 'osmdbt.command': command, 'osmdbt.command.args': `${prettyArgs}` });
    await $`${commandPath} ${args}`;
    handleSpanOnSuccess(span);
  } catch (error) {
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

  logger.debug(`uploading diff of sequence number: ${sequenceNumber} total of ${newDiffAndStatePaths.length} files`);

  const uploads = newDiffAndStatePaths.map(async (filePath, index) => {
    logger.debug(`uploading ${index + 1} out of ${newDiffAndStatePaths.length}, file: ${filePath}`);
    const localPath = path.join(osmdbtConfig.changesDir, filePath);
    const uploadContent = await promisifySpan('fs.read', { 'file.path': localPath }, contextAPI.active(), () => fsPromises.readFile(localPath));
    await putObjectWrapper(filePath, uploadContent);
  });

  span.setAttributes({ 'upload.amount': uploads.length, 'upload.sequenceNumber': sequenceNumber });

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
  try {
    logger.info(`putting key: ${key} into bucket: ${bucketName}, content type: ${contentType ?? 'unknown'}, acl: ${acl}`);
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
    handleSpanOnError(span, error);
    throw new ErrorWithExitCode(
      `failed putting key: ${key} into bucket: ${bucketName} received the following error: ${error}`,
      ExitCodes.PUT_OBJECT_ERROR
    );
  }
};

const commitChanges = async (span) => {
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
  logger.debug(`marking log files for catchup, found ${logFilesNames.length} potential log files`);

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

  span.setAttribute('mark.amount', renameFilesPromises.length);

  try {
    await Promise.all(renameFilesPromises);
  } catch (error) {
    handleSpanOnError(span, error);
    throw error;
  }
  handleSpanOnSuccess(span);
};

const rollback = async (span) => {
  logger.info('something went wrong running rollback');
  singleJobSpan.setAttribute('job.rollback', true);
  try {
    const backupState = await promisifySpan('fs.read', { 'file.path': OSMDBT_STATE_BACKUP_PATH, 'file.name': STATE_FILE }, contextAPI.active(), () =>
      fsPromises.readFile(OSMDBT_STATE_BACKUP_PATH)
    );
    await putObjectWrapper(STATE_FILE, backupState);
    handleSpanOnSuccess(span);
  } catch (error) {
    handleSpanOnError(span, error);
    logger.error(error.message);
    await processExitSafely(ExitCodes.ROLLBACK_FAILURE_ERROR);
  }
};

const processExitSafely = async (exitCode = ExitCodes.GENERAL_ERROR) => {
  logger.info(`exiting safely with exit code: ${exitCode}`);

  let rootSpanStatus;
  rootSpanStatus = exitCode == (ExitCodes.SUCCESS || ExitCodes.TERMINATED) ? SpanStatusCode.OK : SpanStatusCode.ERROR;
  singleJobSpan.setAttributes({ 'job.exitCode': exitCode, 'job.upload.amount': filesUploaded });
  singleJobSpan.setStatus(rootSpanStatus);

  await cleanup();
  process.exit(exitCode);
};

const cleanup = async () => {
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
  logger.info(`new job has started`);
  try {
    await tracer.startActiveSpan('prepare-environment', undefined, contextAPI.active(), prepareEnvironment);

    s3Client = initializeS3Client();

    await tracer.startActiveSpan('get-last-state', undefined, contextAPI.active(), getStateFileFromS3ToFs);
    const lastSequenceNumber = await tracer.startActiveSpan('fs.read', undefined, contextAPI.active(), getSequenceNumber);

    logger.info(`last sequenceNumber: ${lastSequenceNumber} was fetched from object storage`);
    singleJobSpan.setAttribute('job.sequenceNumber.start', lastSequenceNumber);

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
    if (lastSequenceNumber === newSequenceNumber) {
      logger.info(`no diff was found on this job, exiting gracefully`);
      await processExitSafely(ExitCodes.SUCCESS);
    }

    logger.info(`diff was created for sequenceNumber: ${newSequenceNumber}, starting the upload`);
    singleJobSpan.setAttribute('job.sequenceNumber.end', newSequenceNumber);

    await tracer.startActiveSpan('upload-diff', undefined, contextAPI.active(), async (span) => await uploadDiff(newSequenceNumber, span));

    logger.info(`finished the upload of the diff, uploading state file`);

    const newState = await promisifySpan('fs.read', { 'file.path': OSMDBT_STATE_PATH, 'file.name': STATE_FILE }, contextAPI.active(), () =>
      fsPromises.readFile(OSMDBT_STATE_PATH)
    );
    await putObjectWrapper(STATE_FILE, newState);

    logger.info(`finished the upload of the state file, commiting changes`);

    try {
      await tracer.startActiveSpan('commit-changes', undefined, contextAPI.active(), commitChanges);
    } catch (error) {
      await tracer.startActiveSpan('rollback', undefined, contextAPI.active(), rollback);
      singleJobSpan.setAttribute('job.sequenceNumber.end', lastSequenceNumber);
      throw error;
    }

    logger.info(`job completed successfully, exiting gracefully`);
  } catch (error) {
    logger.error(error.message);
    jobExitCode = error.exitCode;
  } finally {
    await processExitSafely(jobExitCode);
  }
};

const mainContext = traceAPI.setSpan(contextAPI.active(), singleJobSpan);
await contextAPI.with(mainContext, main);
