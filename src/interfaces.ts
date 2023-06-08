import { ObjectCannedACL } from '@aws-sdk/client-s3';
import { MediatorConfig } from '@map-colonies/arstotzka-mediator';

export interface OsmdbtConfig {
  changesDir: string;
  runDir: string;
  logDir: string;
  getLogMaxChanges: number;
  verbose: boolean;
}

export interface TracingConfig {
  enabled: boolean;
  url: string;
}

export interface ObjectStorageConfig {
  endpoint: string;
  bucketName: string;
  acl: ObjectCannedACL;
}

export interface ArstotzkaConfig {
  enabled: boolean;
  serviceId: string;
  mediator: MediatorConfig;
}
