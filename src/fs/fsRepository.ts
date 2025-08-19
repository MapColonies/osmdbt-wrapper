import { readFile, mkdir, appendFile, readdir, rename, unlink, writeFile } from 'fs/promises';
import { type Logger } from '@map-colonies/js-logger';
import { inject, injectable } from 'tsyringe';
import { context as contextAPI } from '@opentelemetry/api';
import { SERVICES } from '@src/common/constants';
import { promisifySpan } from '@src/common/tracing/util';
import { FsAttributes, FsSpanName } from '@src/common/tracing/fs';

@injectable()
export class FsRepository {
  public constructor(@inject(SERVICES.LOGGER) private readonly logger: Logger) {}

  public async readFile(filePath: string, encoding?: BufferEncoding): ReturnType<typeof readFile> {
    return promisifySpan(
      FsSpanName.FS_READ,
      { [FsAttributes.FILE_PATH]: filePath, [FsAttributes.FILE_NAME]: filePath.split('/').pop() },
      contextAPI.active(),
      async () => readFile(filePath, encoding)
    );
  }

  public async mkdir(dirPath: string): ReturnType<typeof mkdir> {
    return promisifySpan(FsSpanName.FS_MKDIR, { [FsAttributes.DIR_PATH]: dirPath }, contextAPI.active(), async () =>
      mkdir(dirPath, { recursive: true })
    );
  }

  public async appendFile(filePath: string, data: string, encoding: BufferEncoding): ReturnType<typeof appendFile> {
    return promisifySpan(FsSpanName.FS_APPEND, { [FsAttributes.FILE_PATH]: filePath }, contextAPI.active(), async () =>
      appendFile(filePath, data, encoding)
    );
  }

  public async readdir(dirPath: string): Promise<string[]> {
    return promisifySpan(FsSpanName.FS_READ_DIR, { [FsAttributes.DIR_PATH]: dirPath }, contextAPI.active(), async () => readdir(dirPath));
  }

  public async rename(oldPath: string, newPath: string): ReturnType<typeof rename> {
    return promisifySpan(
      FsSpanName.FS_RENAME,
      { [FsAttributes.FILE_PATH]: oldPath, [FsAttributes.FILE_NAME]: oldPath.split('/').pop() },
      contextAPI.active(),
      async () => rename(oldPath, newPath)
    );
  }

  public async unlink(filePath: string): ReturnType<typeof unlink> {
    return promisifySpan(
      FsSpanName.FS_UNLINK,
      { [FsAttributes.FILE_PATH]: filePath, [FsAttributes.FILE_NAME]: filePath.split('/').pop() },
      contextAPI.active(),
      async () => unlink(filePath)
    );
  }

  public async writeFile(filePath: string, data: string | NodeJS.ArrayBufferView, encoding?: BufferEncoding): ReturnType<typeof writeFile> {
    return promisifySpan(
      FsSpanName.FS_WRITE,
      { [FsAttributes.FILE_PATH]: filePath, [FsAttributes.FILE_NAME]: filePath.split('/').pop() },
      contextAPI.active(),
      async () => writeFile(filePath, data, encoding)
    );
  }
}
