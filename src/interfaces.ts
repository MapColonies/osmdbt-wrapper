import { ObjectCannedACL } from '@aws-sdk/client-s3';

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
