import { writeFile } from 'fs/promises';
import { Counter as PromCounter, Registry as PromRegistry } from 'prom-client';
import { inject, injectable, singleton } from 'tsyringe';
import { context as contextAPI, Span } from '@opentelemetry/api';
import { type Logger } from '@map-colonies/js-logger';
import { SERVICES, STATE_FILE } from '@src/common/constants';
import { type ConfigType } from '@src/common/config';
import { ObjectStorageConfig } from '@src/common/interfaces';
import { handleSpanOnError, handleSpanOnSuccess, promisifySpan } from '@src/common/tracing/util';
import { FsAttributes, FsSpanName } from '@src/common/tracing/fs';
import { streamToString } from '@src/util';
import { S3_REPOSITORY, type S3Repository } from './s3Repository';

@singleton()
@injectable()
export class S3Manager {
  private readonly objectStorageConfig: ObjectStorageConfig;
  private readonly filesCounter?: PromCounter<'rootSpan'>;
  private readonly errorCounter?: PromCounter<'rootSpan'>;
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
        labelNames: ['rootSpan'] as const,
        registers: [registry],
      });
      this.errorCounter = new PromCounter({
        name: 'osmdbt_s3_error_count',
        help: 'The total number of errors encountered while interacting with s3',
        labelNames: ['rootSpan'] as const,
        registers: [registry],
      });
    }
  }

  public async getStateFileFromS3ToFs(
    {
      path,
      backupPath,
    }: {
      path: string;
      backupPath: string;
    },
    span?: Span
  ): Promise<void> {
    this.logger.debug({ msg: 'getting state file from s3' });
    let stateFileStream: NodeJS.ReadStream;

    try {
      stateFileStream = await this.s3Repository.getObjectWrapper(this.objectStorageConfig.bucketName, STATE_FILE);
    } catch (error) {
      handleSpanOnError(span, error, this.errorCounter);
      throw error;
    }

    const stateFileContent = await streamToString(stateFileStream);
    const writeFilesPromises = [path, backupPath].map(async (filePath) => {
      this.logger.debug({ msg: 'writing file', filePath });
      await promisifySpan(FsSpanName.FS_WRITE, { [FsAttributes.FILE_PATH]: filePath }, contextAPI.active(), async () =>
        writeFile(path, stateFileContent)
      );
    });

    try {
      await Promise.all(writeFilesPromises);
    } catch (error) {
      handleSpanOnError(span, error, this.errorCounter);
      throw error;
    }

    handleSpanOnSuccess(span);
  }

  public async uploadFile(fileName: string, buffer: Buffer, span?: Span): Promise<void> {
    this.logger.debug({ msg: 'putting file to s3', fileName });

    try {
      await this.s3Repository.putObjectWrapper(this.objectStorageConfig.bucketName, fileName, buffer);
      this.filesCounter?.inc({ rootSpan: span?.spanContext().traceId });
      handleSpanOnSuccess(span);
    } catch (error) {
      this.logger.error({ err: error, msg: 'failed to put file to s3', fileName });
      handleSpanOnError(span, error, this.errorCounter);
      throw error;
    }
  }
}
