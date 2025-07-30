import { DependencyContainer, FactoryFunction } from 'tsyringe';
import { S3Client } from '@aws-sdk/client-s3';
import { ATTR_RPC_SYSTEM, ATTR_RPC_SERVICE, ATTR_NETWORK_TRANSPORT, ATTR_SERVER_ADDRESS } from '@opentelemetry/semantic-conventions/incubating';
import { Logger } from '@map-colonies/js-logger';
import { Attributes, Tracer } from '@opentelemetry/api';
import { ConfigType } from '@src/common/config';
import { SERVICES } from '../common/constants';
import { ObjectStorageConfig } from '../common/interfaces';
import { S3Attributes } from '../common/tracing/s3';
import { createS3Repositry, S3Repository } from './s3Repository';

export const s3ClientFactory: FactoryFunction<S3Client> = (container: DependencyContainer): S3Client => {
  const config = container.resolve<ConfigType>(SERVICES.CONFIG);
  const logger = container.resolve<Logger>(SERVICES.LOGGER);

  const { endpoint, bucketName, acl, region, credentials } = config.get('objectStorage') as ObjectStorageConfig;
  logger.info({ msg: 'initializing s3 client', endpoint, bucketName, acl });

  return new S3Client({
    endpoint,
    region,
    forcePathStyle: true,
    credentials: {
      accessKeyId: credentials.accessKey,
      secretAccessKey: credentials.secretKey,
    },
  });
};

export const s3RepositoryFactory: FactoryFunction<S3Repository> = (container: DependencyContainer) => {
  const config = container.resolve<ConfigType>(SERVICES.CONFIG);
  const s3Client = container.resolve<S3Client>(SERVICES.S3_CLIENT);
  const tracer = container.resolve<Tracer>(SERVICES.TRACER);
  const logger = container.resolve<Logger>(SERVICES.LOGGER);

  const { endpoint, bucketName, acl, region } = config.get('objectStorage') as ObjectStorageConfig;

  const baseS3SnapAttributes: Attributes = {
    [ATTR_RPC_SYSTEM]: 'aws.api',
    [ATTR_RPC_SERVICE]: 'S3',
    [ATTR_NETWORK_TRANSPORT]: 'ip_tcp',
    [ATTR_SERVER_ADDRESS]: endpoint,
    [S3Attributes.S3_AWS_REGION]: region,
    [S3Attributes.S3_BUCKET_NAME]: bucketName,
  };

  return createS3Repositry(s3Client, baseS3SnapAttributes, acl, tracer, logger);
};
