import { Readable } from 'stream';
import { Registry } from 'prom-client';
import jsLogger from '@map-colonies/js-logger';
import { getConfig, initConfig } from '@src/common/config';
import { S3Manager } from '@src/s3/s3Manager';
import { S3Repository } from '@src/s3/s3Repository';
import { FsRepository } from '@src/fs/fsRepository';

jest.mock('fs/promises');

let s3Manager: S3Manager;
describe('s3Manager', () => {
  const headObjectWrapper = jest.fn();
  const getObjectWrapper = jest.fn();
  const putObjectWrapper = jest.fn();
  const deleteObjectWrapper = jest.fn();

  const writeFile = jest.fn();

  beforeAll(async () => {
    await initConfig(true);
  });

  beforeEach(() => {
    const s3Repository = {
      headObjectWrapper,
      getObjectWrapper,
      deleteObjectWrapper,
      putObjectWrapper,
    } as unknown as S3Repository;

    const mockRegistry = {
      registerMetric: jest.fn(),
      getSingleMetric: jest.fn(),
      metrics: jest.fn(),
      clear: jest.fn(),
    } as unknown as Registry;

    const fsRepository = {
      writeFile,
    } as unknown as FsRepository;

    s3Manager = new S3Manager(s3Repository, getConfig(), jsLogger({ enabled: false }), fsRepository, mockRegistry);
  });

  describe('getStateFileFromS3ToFs', () => {
    it('should run successfully', async () => {
      const stream = new Readable({
        read() {
          this.push('mock-content');
          this.push(null);
        },
      }) as NodeJS.ReadStream;

      getObjectWrapper.mockResolvedValueOnce(stream);

      writeFile.mockResolvedValue(undefined);

      await expect(
        s3Manager.getStateFileFromS3ToFs({
          path: '/mock/path',
          backupPath: '/mock/backup/path',
        })
      ).resolves.toBeUndefined();

      expect(getObjectWrapper).toHaveBeenCalledTimes(1);
      expect(writeFile).toHaveBeenCalledTimes(2);
    });

    it('should fail becuase getObjectWrapper throws error', async () => {
      const error = new Error('some error');
      getObjectWrapper.mockRejectedValueOnce(error);

      await expect(
        s3Manager.getStateFileFromS3ToFs({
          path: '/mock/path',
          backupPath: '/mock/backup/path',
        })
      ).rejects.toBe(error);
    });

    it('should fail becuase writeFile throws error', async () => {
      const error = new Error('some error');
      const stream = new Readable({
        read() {
          this.push('mock-content');
          this.push(null);
        },
      }) as NodeJS.ReadStream;

      getObjectWrapper.mockResolvedValueOnce(stream);

      writeFile.mockRejectedValueOnce(error);

      await expect(
        s3Manager.getStateFileFromS3ToFs({
          path: '/mock/path',
          backupPath: '/mock/backup/path',
        })
      ).rejects.toBe(error);
    });
  });

  describe('uploadFile', () => {
    it('should run successfully', async () => {
      putObjectWrapper.mockResolvedValueOnce(undefined);

      await expect(s3Manager.uploadFile('test', Buffer.from('test'))).resolves.toBeUndefined();

      expect(putObjectWrapper).toHaveBeenCalledTimes(1);
    });

    it('should fail becuase putObjectWrapper throws error', async () => {
      const error = new Error('some error');
      putObjectWrapper.mockRejectedValueOnce(error);

      await expect(s3Manager.uploadFile('test', Buffer.from('test'))).rejects.toBe(error);
    });
  });
});
