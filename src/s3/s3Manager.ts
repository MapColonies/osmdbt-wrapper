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
  private readonly uploadCounter?: PromCounter;
  private readonly errorCounter?: PromCounter;

  public constructor(
    @inject(S3_REPOSITORY) private readonly s3Repository: S3Repository,
    @inject(SERVICES.CONFIG) private readonly config: ConfigType,
    @inject(SERVICES.LOGGER) private readonly logger: Logger,
    @inject(SERVICES.METRICS) registry?: PromRegistry
  ) {
    this.objectStorageConfig = this.config.get('objectStorage') as ObjectStorageConfig;

    if (registry !== undefined) {
      this.uploadCounter = new PromCounter({
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

  public async getFile(fileName: string): Promise<NodeJS.ReadStream> {
    this.logger.debug({ msg: 'getting file from s3' });
    let fileStream: NodeJS.ReadStream;

    try {
      fileStream = await this.s3Repository.getObjectWrapper(this.objectStorageConfig.bucketName, fileName);
    } catch (error) {
      this.errorCounter?.inc();
      this.logger.error({ err: error, msg: 'failed to get file from s3', fileName });
      throw new ErrorWithExitCode('s3 get file error', ExitCodes.S3_ERROR);
    }
    return fileStream;
  }

  public async uploadFile(fileName: string, buffer: Buffer): Promise<void> {
    this.logger.debug({ msg: 'putting file to s3', fileName });

    try {
      await this.s3Repository.putObjectWrapper(this.objectStorageConfig.bucketName, fileName, buffer);
      this.uploadCounter?.inc();
    } catch (error) {
      this.errorCounter?.inc();
      this.logger.error({ err: error, msg: 'failed to put file to s3', fileName });
      throw new ErrorWithExitCode('s3 put file error', ExitCodes.S3_ERROR);
    }
  }
}
