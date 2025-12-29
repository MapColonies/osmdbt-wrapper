import { DependencyContainer, FactoryFunction } from 'tsyringe';
import { S3Client } from '@aws-sdk/client-s3';
import { type Logger } from '@map-colonies/js-logger';
import { type ConfigType } from '@src/common/config';
import { SERVICES } from '../common/constants';
import { ObjectStorageConfig } from '../common/interfaces';
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
  const logger = container.resolve<Logger>(SERVICES.LOGGER);

  const { acl } = config.get('objectStorage') as ObjectStorageConfig;

  return createS3Repositry(s3Client, acl, logger);
};
