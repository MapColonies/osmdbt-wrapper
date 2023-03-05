import config from 'config';
import jsLogger, { LoggerOptions } from '@map-colonies/js-logger';
import { getOtelMixin } from '@map-colonies/telemetry';

const loggerConfig = config.get<LoggerOptions>('telemetry.logger');

export const logger = jsLogger({ ...loggerConfig, mixin: getOtelMixin() });
