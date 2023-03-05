/* eslint-disable @typescript-eslint/naming-convention */ // span attributes and aws-sdk/client-s3 does not follow convention
import config from 'config';
import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  HeadObjectCommandOutput,
  ObjectCannedACL,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { trace as traceAPI, context as contextAPI, Attributes, SpanKind } from '@opentelemetry/api';
import { SemanticAttributes } from '@opentelemetry/semantic-conventions';
import { contentType } from 'mime-types';
import { ExitCodes, S3_NOT_FOUND_ERROR_NAME, S3_REGION } from './constants';
import { ErrorWithExitCode } from './errors';
import { ObjectStorageConfig } from './interfaces';
import { S3Attributes, S3Method, S3SpanName } from './telemetry/tracing/s3';
import { handleSpanOnError, handleSpanOnSuccess, TRACER_NAME } from './telemetry/tracing/util';
import { logger } from './telemetry/logger';

let s3Client: S3Client | undefined;
let baseS3SnapAttributes: Attributes;

const initializeS3Client = (config: ObjectStorageConfig): S3Client => {
  const { endpoint, bucketName, acl } = config;
  logger.info({ msg: 'initializing s3 client', endpoint, bucketName, acl });

  baseS3SnapAttributes = {
    [SemanticAttributes.RPC_SYSTEM]: 'aws.api',
    [SemanticAttributes.RPC_SERVICE]: 'S3',
    [SemanticAttributes.NET_TRANSPORT]: 'ip_tcp',
    [SemanticAttributes.NET_PEER_NAME]: endpoint,
    [S3Attributes.S3_AWS_REGION]: S3_REGION,
    [S3Attributes.S3_BUCKET_NAME]: bucketName,
  };

  return new S3Client({
    endpoint,
    region: S3_REGION,
    forcePathStyle: true,
  });
};

const getClient = (): S3Client => {
  if (s3Client === undefined) {
    const objectStorageConfig = config.get<ObjectStorageConfig>('objectStorage');
    s3Client = initializeS3Client(objectStorageConfig);
  }

  return s3Client;
};

export const headObjectWrapper = async (bucketName: string, key: string): Promise<HeadObjectCommandOutput | undefined> => {
  let span;

  logger.debug({ msg: 'heading object from s3', bucketName, key });

  try {
    span = traceAPI.getTracer(TRACER_NAME).startSpan(
      S3SpanName.HEAD_OBJECT,
      {
        kind: SpanKind.CLIENT,
        attributes: { ...baseS3SnapAttributes, [SemanticAttributes.RPC_METHOD]: S3Method.HEAD_OBJECT, [S3Attributes.S3_KEY]: key },
      },
      contextAPI.active()
    );
    const headObjectResponse = await getClient().send(new HeadObjectCommand({ Bucket: bucketName, Key: key }));
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
};

export const getObjectWrapper = async (bucketName: string, key: string): Promise<NodeJS.ReadStream> => {
  let span;

  logger.debug({ msg: 'getting object from s3', bucketName, key });

  try {
    span = traceAPI.getTracer(TRACER_NAME).startSpan(
      S3SpanName.GET_OBJECT,
      {
        kind: SpanKind.CLIENT,
        attributes: { ...baseS3SnapAttributes, [SemanticAttributes.RPC_METHOD]: S3Method.GET_OBJECT, [S3Attributes.S3_KEY]: key },
      },
      contextAPI.active()
    );
    const commandOutput = await getClient().send(new GetObjectCommand({ Bucket: bucketName, Key: key }));
    handleSpanOnSuccess(span);
    return commandOutput.Body as unknown as NodeJS.ReadStream;
  } catch (error) {
    logger.error({ err: error, msg: 'failed getting key from bucket', bucketName, key });

    handleSpanOnError(span, error);

    throw new ErrorWithExitCode(error instanceof Error ? error.message : `failed getting key: ${key} from bucket: ${bucketName}`, ExitCodes.S3_ERROR);
  }
};

export const putObjectWrapper = async (bucketName: string, key: string, body: Buffer, acl?: ObjectCannedACL): Promise<void> => {
  let evaluatedContentType: string | undefined = undefined;
  const fetchedTypeFromKey = key.split('/').pop();
  if (fetchedTypeFromKey !== undefined) {
    const type = contentType(fetchedTypeFromKey);
    evaluatedContentType = type !== false ? type : undefined;
  }

  let span;

  logger.debug({ msg: 'putting key in bucket', key, bucketName, acl: acl, contentType: evaluatedContentType });

  try {
    span = traceAPI.getTracer(TRACER_NAME).startSpan(
      S3SpanName.PUT_OBJECT,
      {
        kind: SpanKind.CLIENT,
        attributes: {
          ...baseS3SnapAttributes,
          [SemanticAttributes.RPC_METHOD]: S3Method.PUT_OBJECT,
          [S3Attributes.S3_KEY]: key,
          [S3Attributes.S3_CONTENT_TYPE]: evaluatedContentType ?? 'unknown',
          [S3Attributes.S3_ACL]: acl,
        },
      },
      contextAPI.active()
    );

    await getClient().send(new PutObjectCommand({ Bucket: bucketName, Key: key, Body: body, ContentType: evaluatedContentType, ACL: acl }));
    handleSpanOnSuccess(span);
  } catch (error) {
    logger.error({ err: error, msg: 'failed putting key in bucket', acl: acl, bucketName, key });

    handleSpanOnError(span, error);

    throw new ErrorWithExitCode(error instanceof Error ? error.message : `failed putting key: ${key} into bucket: ${bucketName}`, ExitCodes.S3_ERROR);
  }
};

export const deleteObjectWrapper = async (bucketName: string, key: string): Promise<void> => {
  let span;

  logger.debug({ msg: 'deleting object from s3', bucketName, key });

  try {
    span = traceAPI.getTracer(TRACER_NAME).startSpan(
      S3SpanName.DELETE_OBJECT,
      {
        kind: SpanKind.CLIENT,
        attributes: { ...baseS3SnapAttributes, [SemanticAttributes.RPC_METHOD]: S3Method.DELETE_OBJECT, [S3Attributes.S3_KEY]: key },
      },
      contextAPI.active()
    );
    await getClient().send(new DeleteObjectCommand({ Bucket: bucketName, Key: key }));
    handleSpanOnSuccess(span);
  } catch (error) {
    logger.error({ err: error, msg: 'failed deleting key from bucket', bucketName, key });

    handleSpanOnError(span, error);

    throw new ErrorWithExitCode(error instanceof Error ? error.message : `failed getting key: ${key} from bucket: ${bucketName}`, ExitCodes.S3_ERROR);
  }
};
