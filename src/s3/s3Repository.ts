import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  HeadObjectCommandOutput,
  ObjectCannedACL,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { context as contextAPI, SpanKind, Attributes, Tracer } from '@opentelemetry/api';
import { ATTR_RPC_SYSTEM } from '@opentelemetry/semantic-conventions/incubating';
import { Logger } from '@map-colonies/js-logger';
import { ErrorWithExitCode } from '../common/errors';
import { ExitCodes, S3_NOT_FOUND_ERROR_NAME } from '../common/constants';
import { S3Attributes, S3Method, S3SpanName } from '../common/tracing/s3';
import { handleSpanOnError, handleSpanOnSuccess } from '../common/tracing/util';
import { evaluateContentType } from '../util';

export const S3_REPOSITORY = Symbol('S3Repository');
export type S3Repository = ReturnType<typeof createS3Repositry>;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export const createS3Repositry = (client: S3Client, baseS3SnapAttributes: Attributes, aclConfig: ObjectCannedACL, tracer: Tracer, logger: Logger) => {
  return {
    async headObjectWrapper(bucketName: string, key: string): Promise<HeadObjectCommandOutput | undefined> {
      let span;

      logger.debug({ msg: 'heading object from s3', bucketName, key });

      try {
        span = tracer.startSpan(
          S3SpanName.HEAD_OBJECT,
          {
            kind: SpanKind.CLIENT,
            attributes: { ...baseS3SnapAttributes, [ATTR_RPC_SYSTEM]: S3Method.HEAD_OBJECT, [S3Attributes.S3_KEY]: key },
          },
          contextAPI.active()
        );
        // eslint-disable-next-line @typescript-eslint/naming-convention
        const headObjectResponse = await client.send(new HeadObjectCommand({ Bucket: bucketName, Key: key }));
        handleSpanOnSuccess(span);
        return headObjectResponse;
      } catch (error) {
        if (error instanceof Error) {
          if (error.name === S3_NOT_FOUND_ERROR_NAME) {
            handleSpanOnSuccess(span);
            return undefined;
          }
        }

        logger.error({ err: error, msg: 'failed heading object from bucket', bucketName, key });

        handleSpanOnError(span, error);

        throw new ErrorWithExitCode(
          error instanceof Error ? error.message : `failed heading object key: ${key} from bucket: ${bucketName}`,
          ExitCodes.S3_ERROR
        );
      }
    },

    async getObjectWrapper(bucketName: string, key: string): Promise<NodeJS.ReadStream> {
      let span;

      logger.debug({ msg: 'getting object from s3', bucketName, key });

      try {
        span = tracer.startSpan(
          S3SpanName.GET_OBJECT,
          {
            kind: SpanKind.CLIENT,
            attributes: { ...baseS3SnapAttributes, [ATTR_RPC_SYSTEM]: S3Method.GET_OBJECT, [S3Attributes.S3_KEY]: key },
          },
          contextAPI.active()
        );
        // eslint-disable-next-line @typescript-eslint/naming-convention
        const commandOutput = await client.send(new GetObjectCommand({ Bucket: bucketName, Key: key }));
        handleSpanOnSuccess(span);
        return commandOutput.Body as unknown as NodeJS.ReadStream;
      } catch (error) {
        logger.error({ err: error, msg: 'failed getting key from bucket', bucketName, key });

        handleSpanOnError(span, error);

        throw new ErrorWithExitCode(
          error instanceof Error ? error.message : `failed getting key: ${key} from bucket: ${bucketName}`,
          ExitCodes.S3_ERROR
        );
      }
    },

    async putObjectWrapper(bucketName: string, key: string, body: Buffer, aclOverride?: ObjectCannedACL): Promise<void> {
      let span;

      const acl = aclOverride ?? aclConfig;
      const contentType = evaluateContentType(key);

      logger.debug({ msg: 'putting key in bucket', key, bucketName, acl, contentType });

      try {
        span = tracer.startSpan(
          S3SpanName.PUT_OBJECT,
          {
            kind: SpanKind.CLIENT,
            attributes: {
              ...baseS3SnapAttributes,
              [ATTR_RPC_SYSTEM]: S3Method.PUT_OBJECT,
              [S3Attributes.S3_KEY]: key,
              [S3Attributes.S3_CONTENT_TYPE]: contentType ?? 'unknown',
              [S3Attributes.S3_ACL]: acl,
            },
          },
          contextAPI.active()
        );

        // eslint-disable-next-line @typescript-eslint/naming-convention
        await client.send(new PutObjectCommand({ Bucket: bucketName, Key: key, Body: body, ContentType: contentType, ACL: acl }));
        handleSpanOnSuccess(span);
      } catch (error) {
        logger.error({ err: error, msg: 'failed putting key in bucket', acl, bucketName, key });

        handleSpanOnError(span, error);

        throw new ErrorWithExitCode(
          error instanceof Error ? error.message : `failed putting key: ${key} into bucket: ${bucketName}`,
          ExitCodes.S3_ERROR
        );
      }
    },

    async deleteObjectWrapper(bucketName: string, key: string): Promise<void> {
      let span;

      logger.debug({ msg: 'deleting object from s3', bucketName, key });

      try {
        span = tracer.startSpan(
          S3SpanName.DELETE_OBJECT,
          {
            kind: SpanKind.CLIENT,
            attributes: { ...baseS3SnapAttributes, [ATTR_RPC_SYSTEM]: S3Method.DELETE_OBJECT, [S3Attributes.S3_KEY]: key },
          },
          contextAPI.active()
        );
        // eslint-disable-next-line @typescript-eslint/naming-convention
        await client.send(new DeleteObjectCommand({ Bucket: bucketName, Key: key }));
        handleSpanOnSuccess(span);
      } catch (error) {
        logger.error({ err: error, msg: 'failed deleting key from bucket', bucketName, key });

        handleSpanOnError(span, error);

        throw new ErrorWithExitCode(
          error instanceof Error ? error.message : `failed getting key: ${key} from bucket: ${bucketName}`,
          ExitCodes.S3_ERROR
        );
      }
    },
  };
};
