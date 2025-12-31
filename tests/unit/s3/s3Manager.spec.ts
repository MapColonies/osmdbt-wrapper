/* eslint-disable @typescript-eslint/naming-convention */ // due to aws-sdk naming convention
import { Readable } from 'stream';
import jsLogger from '@map-colonies/js-logger';
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Registry } from 'prom-client';
import { getConfig, initConfig } from '@src/common/config';
import { S3Manager } from '@src/s3/s3Manager';
import { createS3Repositry, S3Repository } from '@src/s3/s3Repository';
import { ErrorWithExitCode } from '@src/common/errors';
import { ExitCodes } from '@src/common/constants';
import { streamToString } from '@src/common/util';

const MOCK_KEY = '/mock/key';
const sendMock = jest.fn();

jest.mock('@aws-sdk/client-s3', (): typeof import('@aws-sdk/client-s3') => ({
  ...jest.requireActual('@aws-sdk/client-s3'),
  S3Client: jest.fn().mockImplementation(() => ({
    send: sendMock,
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
    s3Manager = new S3Manager(s3Repository, getConfig(), logger, new Registry());
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('getObject', () => {
    it('should run successfully', async () => {
      sendMock.mockResolvedValueOnce({
        Body: Readable.from(['mock-content']),
      });

      const res = await s3Manager.getObject(MOCK_KEY);

      expect(await streamToString(res)).toBe('mock-content');
      expect(sendMock).toHaveBeenCalledTimes(1);
      expect(sendMock).toHaveBeenCalledWith(expect.objectContaining({ input: { Bucket: s3Manager.bucketName, Key: MOCK_KEY } }));
    });

    it('should throw if s3 client throws error', async () => {
      const error = new Error('some error');
      sendMock.mockRejectedValueOnce(error);

      await expect(s3Manager.getObject(MOCK_KEY)).rejects.toStrictEqual(new ErrorWithExitCode('s3 get object error', ExitCodes.S3_ERROR));

      expect(sendMock).toHaveBeenCalledTimes(1);
      expect(sendMock).toHaveBeenCalledWith(expect.objectContaining({ input: { Bucket: s3Manager.bucketName, Key: MOCK_KEY } }));
    });
  });

  describe('putObject', () => {
    it('should run successfully', async () => {
      const data = Buffer.from('test');
      sendMock.mockResolvedValueOnce(undefined);

      await expect(s3Manager.putObject(MOCK_KEY, data)).resolves.not.toThrow();

      expect(sendMock).toHaveBeenCalledTimes(1);
      expect(sendMock).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({ ACL: 'public-read', Bucket: s3Manager.bucketName, Key: MOCK_KEY }) as PutObjectCommand['input'],
        })
      );
    });

    it('should fail becuase putObjectWrapper throws error', async () => {
      const error = new Error('some error');
      sendMock.mockRejectedValueOnce(error);

      await expect(s3Manager.putObject(MOCK_KEY, Buffer.from('test'))).rejects.toStrictEqual(
        new ErrorWithExitCode('s3 put object error', ExitCodes.S3_ERROR)
      );

      expect(sendMock).toHaveBeenCalledTimes(1);
      expect(sendMock).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({ ACL: 'public-read', Bucket: s3Manager.bucketName, Key: MOCK_KEY }) as PutObjectCommand['input'],
        })
      );
    });
  });
});
