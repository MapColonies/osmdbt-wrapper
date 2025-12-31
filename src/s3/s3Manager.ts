import { Counter as PromCounter, Registry as PromRegistry } from 'prom-client';
import { inject, injectable } from 'tsyringe';
import { type Logger } from '@map-colonies/js-logger';
import { ExitCodes, SERVICES } from '@src/common/constants';
import { type ConfigType } from '@src/common/config';
import { ObjectStorageConfig } from '@src/common/interfaces';
import { ErrorWithExitCode } from '@src/common/errors';
import { S3_REPOSITORY, type S3Repository } from './s3Repository';

@injectable()
export class S3Manager {
  private readonly objectStorageConfig: ObjectStorageConfig;
  private readonly actionCounter?: PromCounter;
  private readonly errorCounter?: PromCounter;

  public constructor(
    @inject(S3_REPOSITORY) private readonly s3Repository: S3Repository,
    @inject(SERVICES.CONFIG) private readonly config: ConfigType,
    @inject(SERVICES.LOGGER) private readonly logger: Logger,
    @inject(SERVICES.METRICS) registry?: PromRegistry
  ) {
    this.objectStorageConfig = this.config.get('objectStorage') as ObjectStorageConfig;

    if (registry !== undefined) {
      this.actionCounter = new PromCounter({
        name: 'osmdbt_objects_count',
        help: 'The total number of successful s3 object actions',
        registers: [registry],
        labelNames: ['kind'] as const,
      });
      this.errorCounter = new PromCounter({
        name: 'osmdbt_s3_error_count',
        help: 'The total number of errors encountered while interacting with s3',
        registers: [registry],
        labelNames: ['kind'] as const,
      });
    }
  }

  public get bucketName(): string {
    return this.objectStorageConfig.bucketName;
  }

  public async getObject(objectName: string): Promise<NodeJS.ReadStream> {
    this.logger.debug({ msg: 'getting object from s3', bucketName: this.bucketName, objectName });
    let objectStream: NodeJS.ReadStream;

    try {
      objectStream = await this.s3Repository.getObjectWrapper(this.bucketName, objectName);
      this.actionCounter?.inc({ kind: 'get' });
      return objectStream;
    } catch (error) {
      this.errorCounter?.inc({ kind: 'get' });
      this.logger.error({ err: error, msg: 'failed to get object from s3', bucketName: this.bucketName, objectName });
      throw new ErrorWithExitCode('s3 get object error', ExitCodes.S3_ERROR);
    }
  }

  public async putObject(objectName: string, buffer: Buffer): Promise<void> {
    this.logger.debug({ msg: 'putting object to s3', bucketName: this.bucketName, objectName });

    try {
      await this.s3Repository.putObjectWrapper(this.bucketName, objectName, buffer);
      this.actionCounter?.inc({ kind: 'put' });
    } catch (error) {
      this.errorCounter?.inc({ kind: 'put' });
      this.logger.error({ err: error, msg: 'failed to put object to s3', bucketName: this.bucketName, objectName });
      throw new ErrorWithExitCode('s3 put object error', ExitCodes.S3_ERROR);
    }
  }
}
