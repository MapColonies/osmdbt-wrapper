/* eslint-disable @typescript-eslint/naming-convention */ // due to aws-sdk naming convention
import { GetObjectCommand, ObjectCannedACL, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { type Logger } from '@map-colonies/js-logger';
import { evaluateContentType } from '../common/util';

export const S3_REPOSITORY = Symbol('S3Repository');
export type S3Repository = ReturnType<typeof createS3Repositry>;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export const createS3Repositry = (client: S3Client, aclConfig: ObjectCannedACL, logger: Logger) => {
  return {
    async getObjectWrapper(bucketName: string, key: string): Promise<NodeJS.ReadStream> {
      logger.debug({ msg: 'getting object from s3', bucketName, key });

      try {
        const commandOutput = await client.send(new GetObjectCommand({ Bucket: bucketName, Key: key }));
        return commandOutput.Body as unknown as NodeJS.ReadStream;
      } catch (error) {
        logger.error({ err: error, msg: 'failed getting key from bucket', bucketName, key });
        throw error;
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
        throw error;
      }
    },
  };
};
