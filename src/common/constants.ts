import { join } from 'path';
import { readPackageJsonSync } from '@map-colonies/read-pkg';

const OSMDBT_BASE_PATH = '/osmdbt';

export const SERVICE_NAME = readPackageJsonSync().name ?? 'unknown_service';
export const DEFAULT_SERVER_PORT = 80;

export const IGNORED_OUTGOING_TRACE_ROUTES = [/^.*\/v1\/metrics.*$/];
export const IGNORED_INCOMING_TRACE_ROUTES = [/^.*\/docs.*$/];

/* eslint-disable @typescript-eslint/naming-convention */
export const SERVICES = {
  LOGGER: Symbol('Logger'),
  CONFIG: Symbol('Config'),
  TRACER: Symbol('Tracer'),
  METRICS: Symbol('METRICS'),
  CLEANUP_REGISTRY: Symbol('CleanupRegistry'),
  ON_SIGNAL: Symbol('OnSignal'),
  S3_CLIENT: Symbol('S3Client'),
  MEDIATOR: Symbol('Mediator'),
} satisfies Record<string, symbol>;
/* eslint-enable @typescript-eslint/naming-convention */

export type Executable = 'osmdbt' | 'osmium';

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
export const S3_NOT_FOUND_ERROR_NAME = 'NotFound';
export const SEQUENCE_NUMBER_COMPONENT_LENGTH = 3;

/* eslint-disable @typescript-eslint/naming-convention */
export const ExitCodes = {
  SUCCESS: 0,
  GENERAL_ERROR: 1,
  OSMDBT_ERROR: 100,
  OSMIUM_ERROR: 101,
  INVALID_STATE_FILE_ERROR: 102,
  ROLLBACK_FAILURE_ERROR: 104,
  S3_ERROR: 105,
  FS_ERROR: 107,
  TERMINATED: 130,
};
/* eslint-enable @typescript-eslint/naming-convention */

export const MILLISECONDS_IN_SECOND = 1000;
// eslint-disable-next-line @typescript-eslint/no-magic-numbers
export const FORCE_SHUTDOWN_TIMEOUT_MS = 10 * MILLISECONDS_IN_SECOND;
