#!/usr/bin/env zx

import { $, path, fs } from 'zx';
import config from 'config';
import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import jsLogger from '@map-colonies/js-logger';

process.on('SIGINT', () => {
  processExitSafely(ExitCodes.TERMINATED);
});

const OSMDBT_CATCHUP = 'osmdbt-catchup';
const OSMDBT_GET_LOG = 'osmdbt-get-log';
const OSMDBT_CREATE_DIFF = 'osmdbt-create-diff';

const OSMDBT_BASE_PATH = '/osmdbt';
const OSMDBT_CONFIG_PATH = path.join(OSMDBT_BASE_PATH, 'config', 'osmdbt-config.yaml');
const OSMDBT_BIN_PATH = path.join(OSMDBT_BASE_PATH, 'build', 'src');
const DIFF_TOP_DIR_DIVIDER = 1000000;
const DIFF_BOTTOM_DIR_DIVIDER = 1000;
const DIFF_STATE_FILE_MODULO = 1000;
const STATE_FILE = 'state.txt';
const OSMDBT_DONE_LOG_PREFIX = '.done';
const DIFF_FILE_EXTENTION = 'osc.gz';
const S3_REGION = 'us-east-1';
const ExitCodes = { SUCCESS: 0, OSMDBT_ERROR: 100, STATE_FETCH_FAILURE_ERROR: 101, INVALID_STATE_FILE: 102, PUT_OBJECT_ERROR: 103, TERMINATED: 130 };

class ErrorWithExitCode extends Error {
  constructor(message, exitCode) {
    super(message);
    this.exitCode = exitCode;
  }
}

const objectStorageConfig = config.get('objectStorage');
const osmdbtConfig = config.get('osmdbt');
const OSMDBT_STATE_PATH = path.join(osmdbtConfig.changesDir, STATE_FILE);
const OSMDBT_STATE_BACKUP_PATH = path.join(osmdbtConfig.changesDir, 'backup', STATE_FILE);
const GLOBAL_OSMDBT_ARGS = osmdbtConfig.verbose ? ['-c', OSMDBT_CONFIG_PATH] : ['-c', OSMDBT_CONFIG_PATH, '-q'];
const logger = jsLogger.default();
let s3Client;

const createDirectory = async (dir) => {
  await $`mkdir -p ${dir}`;
};

const prepareEnvironment = async () => {
  const { logDir, changesDir, runDir } = osmdbtConfig;
  const backupDir = path.join(changesDir, 'backup');
  [logDir, changesDir, runDir, backupDir].forEach(async (dir) => await createDirectory(dir));
};

const streamToString = (stream) => {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
  });
};

const getSequenceNumber = async () => {
  const stateFileContent = await fs.readFile(OSMDBT_STATE_PATH, 'utf8');
  const matchResult = stateFileContent.match(/sequenceNumber=\d+/);
  if (matchResult === null || matchResult.length === 0) {
    throw new ErrorWithExitCode(`failed to fetch sequence number out of the state file, ${STATE_FILE} is invalid`, ExitCodes.INVALID_STATE_FILE);
  }
  return matchResult[0].split('=')[1];
};

const getDiffDirPathComponents = (sequenceNumber) => {
  const top = sequenceNumber / DIFF_TOP_DIR_DIVIDER;
  const bottom = (sequenceNumber % DIFF_TOP_DIR_DIVIDER) / DIFF_BOTTOM_DIR_DIVIDER;
  const state = sequenceNumber % DIFF_STATE_FILE_MODULO;
  return [top, bottom, state].map((component) => {
    const intComponent = parseInt(component);
    return intComponent.toString().padStart(3, '0');
  });
};

const initializeS3Client = () => {
  const { endpoint } = objectStorageConfig;
  return new S3Client({
    signatureVersion: 'v4',
    endpoint,
    region: S3_REGION,
    forcePathStyle: true,
  });
};

const getStateFileFromS3ToFs = async () => {
  const bucketName = objectStorageConfig.bucketName;
  try {
    const stateFileStream = await s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: STATE_FILE }));
    const stateFileContent = await streamToString(stateFileStream.Body);
    await fs.writeFile(OSMDBT_STATE_PATH, stateFileContent);
    await fs.writeFile(OSMDBT_STATE_BACKUP_PATH, stateFileContent);
  } catch (error) {
    throw new ErrorWithExitCode(
      `failed getting key: ${STATE_FILE} from bucket: ${bucketName} received the following error: ${error}`,
      ExitCodes.STATE_FETCH_FAILURE_ERROR
    );
  }
};

const runOsmdbtCommand = async (command, commandArgs = [], onError = undefined) => {
  logger.info(`preparing run of ${command}`);
  try {
    const args = [...GLOBAL_OSMDBT_ARGS, ...commandArgs];
    const commandPath = path.join(OSMDBT_BIN_PATH, command);
    await $`${commandPath} ${args}`;
  } catch (error) {
    if (onError) {
      await onError();
    }
    throw new ErrorWithExitCode(error.stderr, ExitCodes.OSMDBT_ERROR);
  }
};

const putObjectWrapper = async (key, body) => {
  const { bucketName, acl } = objectStorageConfig;
  try {
    logger.info(`putting key: ${key} into bucket: ${bucketName}`);
    await s3Client.send(new PutObjectCommand({ Bucket: bucketName, Key: key, Body: body, ACL: acl }));
  } catch (error) {
    throw new ErrorWithExitCode(
      `failed putting key: ${key} into bucket: ${bucketName} received the following error: ${error}`,
      ExitCodes.PUT_OBJECT_ERROR
    );
  }
};

const changeLogFilesToUndone = async () => {
  const { logDir } = osmdbtConfig;
  const logFilesNames = await fs.readdir(logDir);
  logFilesNames.forEach(async (logFileName) => {
    if (!logFileName.endsWith(OSMDBT_DONE_LOG_PREFIX)) {
      return;
    }
    const logFileNameUndone = logFileName.slice(0, logFileName.length - OSMDBT_DONE_LOG_PREFIX.length);
    const currentPath = path.join(logDir, logFileName);
    const newPath = path.join(logDir, logFileNameUndone);
    await fs.rename(currentPath, newPath);
  });
};

const processExitSafely = (exitCode) => {
  logger.info(`exiting safely with exit code: ${exitCode}`);

  if (s3Client !== undefined) {
    s3Client.destroy();
  }
  process.exit(exitCode);
};

const rollback = async () => {
  logger.info('something went wrong running rollback');
  await putObjectWrapper(STATE_FILE, fs.createReadStream(OSMDBT_STATE_BACKUP_PATH));
}

logger.info(`new job has started`);
try {
  await prepareEnvironment();

  s3Client = initializeS3Client();
  await getStateFileFromS3ToFs();
  const lastSequenceNumber = await getSequenceNumber();

  logger.info(`last sequenceNumber: ${lastSequenceNumber} was fetched from object storage`);

  await runOsmdbtCommand(OSMDBT_GET_LOG, ['-m', osmdbtConfig.getLogMaxChanges]);
  await runOsmdbtCommand(OSMDBT_CREATE_DIFF);

  const newSequenceNumber = await getSequenceNumber();
  if (lastSequenceNumber === newSequenceNumber) {
    logger.info(`no diff was found on this job, exiting gracefully`);
    processExitSafely(ExitCodes.SUCCESS);
  }

  logger.info(`diff was created for sequenceNumber: ${newSequenceNumber}, starting the upload`);

  const [top, bottom, stateNumber] = getDiffDirPathComponents(newSequenceNumber);
  const newDiffAndStatePaths = [STATE_FILE, DIFF_FILE_EXTENTION].map((fileExtention) => path.join(top, bottom, `${stateNumber}.${fileExtention}`));
  newDiffAndStatePaths.forEach(async (filePath) => {
    const localPath = path.join(osmdbtConfig.changesDir, filePath);
    const fileStream = fs.createReadStream(localPath);
    await putObjectWrapper(filePath, fileStream);
  });

  logger.info(`finished the upload of the diff, uploading state file`);

  await putObjectWrapper(STATE_FILE, fs.createReadStream(OSMDBT_STATE_PATH));

  await changeLogFilesToUndone();
  await runOsmdbtCommand(OSMDBT_CATCHUP, [], rollback);

  logger.info(`job completed successfully, exiting gracefully`);

  processExitSafely(ExitCodes.SUCCESS);
} catch (error) {
  logger.error(error.message);
  processExitSafely(error.exitCode);
}
