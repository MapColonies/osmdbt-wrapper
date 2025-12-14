import { readFile, mkdir, appendFile, readdir, rename, unlink, writeFile } from 'fs/promises';
import { type Logger } from '@map-colonies/js-logger';
import { inject, injectable } from 'tsyringe';
import { context as contextAPI } from '@opentelemetry/api';
import { ExitCodes, SERVICES } from '@src/common/constants';
import { promisifySpan } from '@src/common/tracing/util';
import { FsAttributes, FsSpanName } from '@src/common/tracing/fs';
import { ErrorWithExitCode } from '@src/common/errors';

@injectable()
export class FsRepository {
  public constructor(@inject(SERVICES.LOGGER) private readonly logger: Logger) {}

  public async readFile(filePath: string, encoding?: BufferEncoding): Promise<ReturnType<typeof readFile>> {
    try {
      return await promisifySpan(
        FsSpanName.FS_READ,
        { [FsAttributes.FILE_PATH]: filePath, [FsAttributes.FILE_NAME]: filePath.split('/').pop() },
        contextAPI.active(),
        async () => readFile(filePath, encoding)
      );
    } catch (error) {
      this.logger.error({ msg: 'read file failed', filePath, err: error });
      throw new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR);
    }
  }

  public async mkdir(dirPath: string): Promise<ReturnType<typeof mkdir>> {
    try {
      return await promisifySpan(FsSpanName.FS_MKDIR, { [FsAttributes.DIR_PATH]: dirPath }, contextAPI.active(), async () =>
        mkdir(dirPath, { recursive: true })
      );
    } catch (error) {
      this.logger.error({ msg: 'mkdir failed', dirPath, err: error });
      throw new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR);
    }
  }

  public async appendFile(filePath: string, data: string, encoding: BufferEncoding): Promise<ReturnType<typeof appendFile>> {
    try {
      return await promisifySpan(FsSpanName.FS_APPEND, { [FsAttributes.FILE_PATH]: filePath }, contextAPI.active(), async () =>
        appendFile(filePath, data, encoding)
      );
    } catch (error) {
      this.logger.error({ msg: 'append file failed', filePath, err: error });
      throw new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR);
    }
  }

  public async readdir(dirPath: string): Promise<string[]> {
    try {
      return await promisifySpan(FsSpanName.FS_READ_DIR, { [FsAttributes.DIR_PATH]: dirPath }, contextAPI.active(), async () => readdir(dirPath));
    } catch (error) {
      this.logger.error({ msg: 'readdir failed', dirPath, err: error });
      throw new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR);
    }
  }

  public async rename(oldPath: string, newPath: string): Promise<ReturnType<typeof rename>> {
    try {
      return await promisifySpan(
        FsSpanName.FS_RENAME,
        { [FsAttributes.FILE_PATH]: oldPath, [FsAttributes.FILE_NAME]: oldPath.split('/').pop() },
        contextAPI.active(),
        async () => rename(oldPath, newPath)
      );
    } catch (error) {
      this.logger.error({ msg: 'rename failed', oldPath, newPath, err: error });
      throw new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR);
    }
  }

  public async unlink(filePath: string): Promise<ReturnType<typeof unlink>> {
    try {
      return await promisifySpan(
        FsSpanName.FS_UNLINK,
        { [FsAttributes.FILE_PATH]: filePath, [FsAttributes.FILE_NAME]: filePath.split('/').pop() },
        contextAPI.active(),
        async () => unlink(filePath)
      );
    } catch (error) {
      this.logger.error({ msg: 'unlink failed', filePath, err: error });
      throw new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR);
    }
  }

  public async writeFile(filePath: string, data: string | NodeJS.ArrayBufferView, encoding?: BufferEncoding): Promise<ReturnType<typeof writeFile>> {
    try {
      return await promisifySpan(
        FsSpanName.FS_WRITE,
        { [FsAttributes.FILE_PATH]: filePath, [FsAttributes.FILE_NAME]: filePath.split('/').pop() },
        contextAPI.active(),
        async () => writeFile(filePath, data, encoding)
      );
    } catch (error) {
      this.logger.error({ msg: 'write file failed', filePath, err: error });
      throw new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR);
    }
  }
}
