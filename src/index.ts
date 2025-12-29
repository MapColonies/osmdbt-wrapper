// this import must be called before the first import of tsyringe
import 'reflect-metadata';
import { createServer } from 'http';
import { createTerminus } from '@godaddy/terminus';
import { type Logger } from '@map-colonies/js-logger';
import { DependencyContainer } from 'tsyringe';
import { ON_SIGNAL, SERVICES } from '@common/constants';
import { type ConfigType } from '@common/config';
import { getApp } from './app';
import { isSingleTask, OSMDBT_PROCESSOR, OsmdbtProcessor } from './osmdbt';
import { stubHealthCheck } from './common/util';

let depContainer: DependencyContainer | undefined;

void getApp()
  .then(async ([app, container]) => {
    depContainer = container;
    const logger = container.resolve<Logger>(SERVICES.LOGGER);
    const config = container.resolve<ConfigType>(SERVICES.CONFIG);

    const port = config.get('server.port');

    const server = createTerminus(createServer(app), {
      healthChecks: { '/liveness': stubHealthCheck },
      onSignal: container.resolve(ON_SIGNAL),
    });

    const osmdbtProcess = container.resolve<OsmdbtProcessor>(OSMDBT_PROCESSOR);

    if (isSingleTask(osmdbtProcess)) {
      await osmdbtProcess();

      const shutDown = container.resolve<() => Promise<void>>(ON_SIGNAL);
      await shutDown();

      server.close();
      return;
    }

    await osmdbtProcess.execute();

    server.listen(port, () => {
      logger.info({ msg: `app started on port ${port}` });
    });
  })
  .catch(async (error: Error) => {
    const errorLogger =
      depContainer?.isRegistered(SERVICES.LOGGER) == true
        ? depContainer.resolve<Logger>(SERVICES.LOGGER).error.bind(depContainer.resolve<Logger>(SERVICES.LOGGER))
        : console.error;
    errorLogger({ msg: '😢 - failed initializing the server', err: error });

    if (depContainer?.isRegistered(ON_SIGNAL) == true) {
      const shutDown: () => Promise<void> = depContainer.resolve(ON_SIGNAL);
      await shutDown();
    }
    process.exit(1);
  });
