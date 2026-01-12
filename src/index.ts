// this import must be called before the first import of tsyringe
import 'reflect-metadata';
import { createServer } from 'node:http';
import { type Logger } from '@map-colonies/js-logger';
import { DependencyContainer } from 'tsyringe';
import { Registry } from 'prom-client';
import { metricsMiddleware } from '@map-colonies/telemetry/prom-metrics';
import { CleanupRegistry } from '@map-colonies/cleanup-registry';
import { TERMINUS_FACTORY, ON_SIGNAL, SERVICES } from '@common/constants';
import { type ConfigType } from '@common/config';
import { getApp } from './app';
import { isSingleTask, OSMDBT_PROCESSOR, OsmdbtProcessor } from './osmdbt';
import { TerminusFactory } from './common/liveness';

let depContainer: DependencyContainer | undefined;

const shutDownFn = async (): Promise<void> => {
  if (depContainer?.isRegistered(ON_SIGNAL) === true) {
    const onSignalFn: () => Promise<void> = depContainer.resolve(ON_SIGNAL);
    return onSignalFn();
  }
};

void getApp()
  .then(async ([app, container]) => {
    depContainer = container;

    const logger = container.resolve<Logger>(SERVICES.LOGGER);
    const config = container.resolve<ConfigType>(SERVICES.CONFIG);
    const registry = container.resolve<Registry>(SERVICES.METRICS);
    const cleanupRegistry = container.resolve<CleanupRegistry>(SERVICES.CLEANUP_REGISTRY);
    const osmdbtProcess = container.resolve<OsmdbtProcessor>(OSMDBT_PROCESSOR);
    const terminusFactory = container.resolve<TerminusFactory>(TERMINUS_FACTORY);

    app.use('/metrics', metricsMiddleware(registry));

    const server = terminusFactory(createServer(app));

    cleanupRegistry.register({
      id: 'server',
      func: async () => {
        return new Promise((resolve) => {
          server.once('close', resolve);
          server.close();
        });
      },
    });

    if (isSingleTask(osmdbtProcess)) {
      await osmdbtProcess();
      await shutDownFn();
      return;
    }

    const port = config.get('server.port');

    server.listen(port, () => {
      logger.info(`liveness on port ${port}`);
    });

    await osmdbtProcess.execute();
  })
  .catch(async (error: Error) => {
    const errorLogger =
      depContainer?.isRegistered(SERVICES.LOGGER) == true
        ? depContainer.resolve<Logger>(SERVICES.LOGGER).error.bind(depContainer.resolve<Logger>(SERVICES.LOGGER))
        : console.error;
    errorLogger({ msg: '😢 - failed initializing the server', err: error });

    await shutDownFn();

    process.exit(1);
  });
