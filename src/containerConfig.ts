import { getOtelMixin } from '@map-colonies/telemetry';
import { CleanupRegistry } from '@map-colonies/cleanup-registry';
import { StatefulMediator } from '@map-colonies/arstotzka-mediator';
import { instancePerContainerCachingFactory } from 'tsyringe';
import { trace } from '@opentelemetry/api';
import { Registry } from 'prom-client';
import { DependencyContainer } from 'tsyringe/dist/typings/types';
import jsLogger, { Logger } from '@map-colonies/js-logger';
import { InjectionObject, registerDependencies } from '@common/dependencyRegistration';
import { SERVICES, SERVICE_NAME } from '@common/constants';
import { getTracing } from '@common/tracing';
import { ConfigType, getConfig } from './common/config';
import { s3ClientFactory, s3RepositoryFactory } from './s3';
import { S3_REPOSITORY } from './s3/s3Repository';
import { OSMDBT_PROCESSOR, OsmdbtProcessor, osmdbtProcessorFactory } from './osmdbt';

export interface RegisterOptions {
  override?: InjectionObject<unknown>[];
  useChild?: boolean;
}

export const registerExternalValues = async (options?: RegisterOptions): Promise<DependencyContainer> => {
  const cleanupRegistry = new CleanupRegistry();

  const dependencies: InjectionObject<unknown>[] = [
    { token: SERVICES.CONFIG, provider: { useValue: getConfig() } },
    {
      token: SERVICES.LOGGER,
      provider: {
        useFactory: instancePerContainerCachingFactory((container) => {
          const config = container.resolve<ConfigType>(SERVICES.CONFIG);

          const loggerConfig = config.get('telemetry.logger');
          const logger = jsLogger({ ...loggerConfig, prettyPrint: loggerConfig.prettyPrint, mixin: getOtelMixin() });

          const cleanupRegistryLogger = logger.child({ subComponent: 'cleanupRegistry' });
          cleanupRegistry.on('itemFailed', (id, error, msg) => cleanupRegistryLogger.error({ msg, itemId: id, err: error }));
          cleanupRegistry.on('finished', (status) => cleanupRegistryLogger.info({ msg: `cleanup registry finished cleanup`, status }));

          return logger;
        }),
      },
    },
    {
      token: SERVICES.TRACER,
      provider: {
        useFactory: instancePerContainerCachingFactory((container) => {
          const cleanupRegistry = container.resolve<CleanupRegistry>(SERVICES.CLEANUP_REGISTRY);
          cleanupRegistry.register({ id: SERVICES.TRACER, func: getTracing().stop.bind(getTracing()) });
          const tracer = trace.getTracer(SERVICE_NAME);
          return tracer;
        }),
      },
    },
    {
      token: SERVICES.METRICS,
      provider: {
        useFactory: instancePerContainerCachingFactory((container) => {
          const metricsRegistry = new Registry();
          const config = container.resolve<ConfigType>(SERVICES.CONFIG);
          config.initializeMetrics(metricsRegistry);
          return metricsRegistry;
        }),
      },
    },
    {
      token: SERVICES.CLEANUP_REGISTRY,
      provider: { useValue: cleanupRegistry },
    },
    {
      token: SERVICES.ON_SIGNAL,
      provider: {
        useValue: {
          useValue: cleanupRegistry.trigger.bind(cleanupRegistry),
        },
      },
    },
    {
      token: SERVICES.S3_CLIENT,
      provider: {
        useFactory: instancePerContainerCachingFactory(s3ClientFactory),
      },
    },
    {
      token: S3_REPOSITORY,
      provider: {
        useFactory: s3RepositoryFactory,
      },
    },
    {
      token: SERVICES.MEDIATOR,
      provider: {
        useFactory: instancePerContainerCachingFactory((container) => {
          const config = container.resolve<ConfigType>(SERVICES.CONFIG);
          const logger = container.resolve<Logger>(SERVICES.LOGGER);

          const arstotzkaConfig = config.get('arstotzka');

          if (!arstotzkaConfig?.enabled) {
            const msg = 'Mediator is not enabled, but it is required for the application to run';
            logger.fatal({ msg });
            throw new Error(msg);
          }

          return new StatefulMediator({ ...arstotzkaConfig.mediator, serviceId: arstotzkaConfig.serviceId, logger });
        }),
      },
    },
    {
      token: OSMDBT_PROCESSOR,
      provider: {
        useFactory: osmdbtProcessorFactory,
      },
      postInjectionHook: (container: DependencyContainer): void => {
        const cleanupRegistry = container.resolve<CleanupRegistry>(SERVICES.CLEANUP_REGISTRY);
        const osmdbtProcessor = container.resolve<OsmdbtProcessor>(OSMDBT_PROCESSOR);

        cleanupRegistry.register({
          id: OSMDBT_PROCESSOR,
          func: async () => {
            const osmdbtProcess = await osmdbtProcessor();
            if (typeof osmdbtProcess === 'object') {
              await osmdbtProcess.destroy();
            }
          },
        });
      },
    },
  ];

  return Promise.resolve(registerDependencies(dependencies, options?.override, options?.useChild));
};
