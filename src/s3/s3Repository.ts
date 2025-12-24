/* eslint-disable @typescript-eslint/naming-convention */ // due to aws-sdk naming convention
import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  HeadObjectCommandOutput,
  ObjectCannedACL,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { Logger } from '@map-colonies/js-logger';
import { ErrorWithExitCode } from '../common/errors';
import { ExitCodes, S3_NOT_FOUND_ERROR_NAME } from '../common/constants';
import { evaluateContentType } from '../common/util';

export const S3_REPOSITORY = Symbol('S3Repository');
export type S3Repository = ReturnType<typeof createS3Repositry>;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export const createS3Repositry = (client: S3Client, aclConfig: ObjectCannedACL, logger: Logger) => {
  return {
    async headObjectWrapper(bucketName: string, key: string): Promise<HeadObjectCommandOutput | undefined> {
      logger.debug({ msg: 'heading object from s3', bucketName, key });

      try {
        const headObjectResponse = await client.send(new HeadObjectCommand({ Bucket: bucketName, Key: key }));
        return headObjectResponse;
      } catch (error) {
        if (error instanceof Error) {
          if (error.name === S3_NOT_FOUND_ERROR_NAME) {
            return undefined;
          }
        }

        logger.error({ err: error, msg: 'failed heading object from bucket', bucketName, key });

        throw new ErrorWithExitCode(
          error instanceof Error ? error.message : `failed heading object key: ${key} from bucket: ${bucketName}`,
          ExitCodes.S3_ERROR
        );
      }
    },

    async getObjectWrapper(bucketName: string, key: string): Promise<NodeJS.ReadStream> {
      logger.debug({ msg: 'getting object from s3', bucketName, key });

      try {
        const commandOutput = await client.send(new GetObjectCommand({ Bucket: bucketName, Key: key }));
        return commandOutput.Body as unknown as NodeJS.ReadStream;
      } catch (error) {
        logger.error({ err: error, msg: 'failed getting key from bucket', bucketName, key });

        throw new ErrorWithExitCode(
          error instanceof Error ? error.message : `failed getting key: ${key} from bucket: ${bucketName}`,
          ExitCodes.S3_ERROR
        );
      }
    },

    async putObjectWrapper(bucketName: string, key: string, body: Buffer, aclOverride?: ObjectCannedACL): Promise<void> {
      const acl = aclOverride ?? aclConfig;
      const contentType = evaluateContentType(key);

      logger.debug({ msg: 'putting key in bucket', key, bucketName, acl, contentType });

      try {
        await client.send(new PutObjectCommand({ Bucket: bucketName, Key: key, Body: body, ContentType: contentType, ACL: acl }));
      } catch (error) {
        logger.error({ err: error, msg: 'failed putting key in bucket', acl, bucketName, key });

        throw new ErrorWithExitCode(
          error instanceof Error ? error.message : `failed putting key: ${key} into bucket: ${bucketName}`,
          ExitCodes.S3_ERROR
        );
      }
    },

    async deleteObjectWrapper(bucketName: string, key: string): Promise<void> {
      logger.debug({ msg: 'deleting object from s3', bucketName, key });

      try {
        await client.send(new DeleteObjectCommand({ Bucket: bucketName, Key: key }));
      } catch (error) {
        logger.error({ err: error, msg: 'failed deleting key from bucket', bucketName, key });

        throw new ErrorWithExitCode(
          error instanceof Error ? error.message : `failed getting key: ${key} from bucket: ${bucketName}`,
          ExitCodes.S3_ERROR
        );
      }
    },
  };
};
