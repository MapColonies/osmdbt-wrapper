import { ObjectCannedACL } from '@aws-sdk/client-s3';
import { MediatorConfig } from '@map-colonies/arstotzka-mediator';

export interface IConfig {
  get: <T>(setting: string) => T;
  has: (setting: string) => boolean;
}

export interface OsmdbtConfig {
  changesDir: string;
  runDir: string;
  logDir: string;
  getLogMaxChanges: number;
  verbose: boolean;
}

export interface OsmiumConfig {
  verbose: boolean;
  progress: boolean;
}

export interface AppConfig {
  shouldCollectInfo: boolean;
  cron?: { enabled: true; expression: string; failurePenalty: number } | { enabled: false };
}

export interface TracingConfig {
  enabled: boolean;
  url: string;
}

export interface ObjectStorageConfig {
  endpoint: string;
  bucketName: string;
  acl: ObjectCannedACL;
  credentials: {
    accessKey: string;
    secretKey: string;
  };
  region: string;
}

export interface ArstotzkaConfig {
  enabled: boolean;
  serviceId: string;
  mediator: MediatorConfig;
}

export interface MetricsConfig {
  buckets: {
    osmdbtJobDurationSeconds: number[];
    osmdbtCommandDurationSeconds: number[];
  };
}
