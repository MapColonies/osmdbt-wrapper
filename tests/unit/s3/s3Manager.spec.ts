import { Readable } from 'stream';
import { Registry } from 'prom-client';
import jsLogger from '@map-colonies/js-logger';
import { getConfig, initConfig } from '@src/common/config';
import { S3Manager } from '@src/s3/s3Manager';
import { createS3Repositry, S3Repository } from '@src/s3/s3Repository';
import { ErrorWithExitCode } from '@src/common/errors';
import { ExitCodes } from '@src/common/constants';
import { S3Client } from '@aws-sdk/client-s3';

// eslint-disable-next-line @typescript-eslint/no-unsafe-return
jest.mock('@aws-sdk/client-s3', () => ({
  ...jest.requireActual('@aws-sdk/client-s3'),

  S3Client: jest.fn().mockImplementation(() => ({
    send: jest.fn(),
  })),
}));

describe('s3Manager', () => {
  let s3Manager: S3Manager;
  let s3Repository: S3Repository;
  let mockedS3Client: jest.Mocked<S3Client>;

  beforeAll(async () => {
    await initConfig(true);

    const logger = jsLogger({ enabled: false });
    mockedS3Client = new S3Client({}) as jest.Mocked<S3Client>;
    s3Repository = createS3Repositry(mockedS3Client, 'public-read', logger);
    s3Manager = new S3Manager(s3Repository, getConfig(), logger);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('getFile', () => {
    it.only('should run successfully', async () => {
      const stream = {
        Body: new Readable({
          read() {
            this.push('mock-content');
            this.push(null);
          },
        }) as NodeJS.ReadStream,
      };

      mockedS3Client.send.mockResolvedValueOnce(stream as never);

      await expect(s3Manager.getFile('/mock/path')).resolves.toBeDefined();

      expect(mockedS3Client.send).toHaveBeenCalledTimes(1);
      expect(s3Repository.getObjectWrapper).toHaveBeenCalledTimes(1);
      expect(s3Repository.getObjectWrapper).toHaveBeenCalledWith(s3Manager['objectStorageConfig'].bucketName, '/mock/path');
    });

    // it('should fail becuase getObjectWrapper throws error', async () => {
    //   const error = new Error('some error');
    //   getObjectWrapper.mockRejectedValueOnce(error);

    //   await expect(s3Manager.getFile('/mock/path')).rejects.toStrictEqual(new ErrorWithExitCode('s3 get file error', ExitCodes.S3_ERROR));
    // });
  });

  describe('uploadFile', () => {
    // it('should run successfully', async () => {
    //   putObjectWrapper.mockResolvedValueOnce(undefined);
    //   await expect(s3Manager.uploadFile('test', Buffer.from('test'))).resolves.toBeUndefined();
    //   expect(putObjectWrapper).toHaveBeenCalledTimes(1);
    // });
    // it('should fail becuase putObjectWrapper throws error', async () => {
    //   const error = new Error('some error');
    //   putObjectWrapper.mockRejectedValueOnce(error);
    //   await expect(s3Manager.uploadFile('test', Buffer.from('test'))).rejects.toStrictEqual(
    //     new ErrorWithExitCode('s3 put file error', ExitCodes.S3_ERROR)
    //   );
    // });
  });
});
