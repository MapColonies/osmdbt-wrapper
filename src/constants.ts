import { join } from 'path';

const OSMDBT_BASE_PATH = '/osmdbt';

export enum OsmdbtCommand {
  GET_LOG = 'osmdbt-get-log',
  CREATE_DIFF = 'osmdbt-create-diff',
  CATCHUP = 'osmdbt-catchup',
}

export const OSMDBT_CONFIG_PATH = join(OSMDBT_BASE_PATH, 'config', 'osmdbt-config.yaml');
export const OSMDBT_BIN_PATH = join(OSMDBT_BASE_PATH, 'build', 'src');
export const DIFF_TOP_DIR_DIVIDER = 1000000;
export const DIFF_BOTTOM_DIR_DIVIDER = 1000;
export const DIFF_STATE_FILE_MODULO = 1000;
export const STATE_FILE = 'state.txt';
export const OSMDBT_DONE_LOG_PREFIX = '.done';
export const DIFF_FILE_EXTENTION = 'osc.gz';
export const BACKUP_DIR_NAME = 'backup';
export const S3_REGION = 'us-east-1';
export const S3_NOT_FOUND_ERROR_NAME = 'NotFound';
export const S3_LOCK_FILE_NAME = 'lockfile';
export const SEQUENCE_NUMBER_COMPONENT_LENGTH = 3;

/* eslint-disable @typescript-eslint/naming-convention */
export const ExitCodes = {
  SUCCESS: 0,
  GENERAL_ERROR: 1,
  OSMDBT_ERROR: 100,
  INVALID_STATE_FILE_ERROR: 102,
  ROLLBACK_FAILURE_ERROR: 104,
  S3_ERROR: 105,
  S3_LOCKED_ERROR: 106,
  TERMINATED: 130,
};
/* eslint-enable @typescript-eslint/naming-convention */
