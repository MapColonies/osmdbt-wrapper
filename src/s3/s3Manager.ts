import { Counter as PromCounter, Registry as PromRegistry } from 'prom-client';
import { inject, injectable, singleton } from 'tsyringe';
import { Span } from '@opentelemetry/api';
import { type Logger } from '@map-colonies/js-logger';
import { SERVICES } from '@src/common/constants';
import { type ConfigType } from '@src/common/config';
import { ObjectStorageConfig } from '@src/common/interfaces';
import { handleSpanOnError, handleSpanOnSuccess } from '@src/common/tracing/util';
import { FsRepository } from '@src/fs/fsRepository';
import { S3_REPOSITORY, type S3Repository } from './s3Repository';

@singleton()
@injectable()
export class S3Manager {
  private readonly objectStorageConfig: ObjectStorageConfig;
  private readonly filesCounter?: PromCounter;
  private readonly errorCounter?: PromCounter;

  public constructor(
    @inject(S3_REPOSITORY) private readonly s3Repository: S3Repository,
    @inject(SERVICES.CONFIG) private readonly config: ConfigType,
    @inject(SERVICES.LOGGER) private readonly logger: Logger,
    @inject(SERVICES.METRICS) registry?: PromRegistry
  ) {
    this.objectStorageConfig = this.config.get('objectStorage') as ObjectStorageConfig;

    if (registry !== undefined) {
      this.filesCounter = new PromCounter({
        name: 'osmdbt_files_count',
        help: 'The total number of files uploaded to s3',
        registers: [registry],
      });
      this.errorCounter = new PromCounter({
        name: 'osmdbt_s3_error_count',
        help: 'The total number of errors encountered while interacting with s3',
        registers: [registry],
      });
    }
  }

  public async getFile(fileName: string, span?: Span): Promise<NodeJS.ReadStream> {
    this.logger.debug({ msg: 'getting file from s3' });
    let fileStream: NodeJS.ReadStream;

    try {
      fileStream = await this.s3Repository.getObjectWrapper(this.objectStorageConfig.bucketName, fileName);
    } catch (error) {
      handleSpanOnError(span, error, this.errorCounter);
      throw error;
    }
    handleSpanOnSuccess(span);
    return fileStream;
  }

  public async uploadFile(fileName: string, buffer: Buffer, span?: Span): Promise<void> {
    this.logger.debug({ msg: 'putting file to s3', fileName });

    try {
      await this.s3Repository.putObjectWrapper(this.objectStorageConfig.bucketName, fileName, buffer);
      this.filesCounter?.inc();
      handleSpanOnSuccess(span);
    } catch (error) {
      this.logger.error({ err: error, msg: 'failed to put file to s3', fileName });
      handleSpanOnError(span, error, this.errorCounter);
      throw error;
    }
  }
}
