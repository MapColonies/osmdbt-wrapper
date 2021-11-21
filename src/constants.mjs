export const OSMDBT_GET_LOG = 'osmdbt-get-log';
export const OSMDBT_CREATE_DIFF = 'osmdbt-create-diff';
export const OSMDBT_CATCHUP = 'osmdbt-catchup';

const OSMDBT_BASE_PATH = '/osmdbt';
export const OSMDBT_CONFIG_PATH = path.join(OSMDBT_BASE_PATH, 'config', 'osmdbt-config.yaml');
export const OSMDBT_BIN_PATH = path.join(OSMDBT_BASE_PATH, 'build', 'src');
export const DIFF_TOP_DIR_DIVIDER = 1000000;
export const DIFF_BOTTOM_DIR_DIVIDER = 1000;
export const DIFF_STATE_FILE_MODULO = 1000;
export const STATE_FILE = 'state.txt';
export const OSMDBT_DONE_LOG_PREFIX = '.done';
export const DIFF_FILE_EXTENTION = 'osc.gz';
export const S3_REGION = 'us-east-1';
export const ExitCodes = {
  SUCCESS: 0,
  GENERAL_ERROR: 1,
  OSMDBT_ERROR: 100,
  STATE_FETCH_FAILURE_ERROR: 101,
  INVALID_STATE_FILE_ERROR: 102,
  PUT_OBJECT_ERROR: 103,
  ROLLBACK_FAILURE_ERROR: 104,
  TERMINATED: 130,
};
