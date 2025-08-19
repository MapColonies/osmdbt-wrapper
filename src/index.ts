// this import must be called before the first import of tsyringe
import 'reflect-metadata';
import { createServer } from 'http';
import { createTerminus } from '@godaddy/terminus';
import { Logger } from '@map-colonies/js-logger';
import { FORCE_SHUTDOWN_TIMEOUT_MS, SERVICES } from '@common/constants';
import { ConfigType } from '@common/config';
import { getApp } from './app';
import { isSingleTask, OSMDBT_PROCESSOR, OsmdbtProcessor } from './osmdbt';

void getApp()
  .then(async ([app, container]) => {
    const logger = container.resolve<Logger>(SERVICES.LOGGER);
    const config = container.resolve<ConfigType>(SERVICES.CONFIG);

    const port = config.get('server.port');

    const stubHealthCheck = async (): Promise<void> => Promise.resolve();
    const server = createTerminus(createServer(app), {
      healthChecks: { '/liveness': stubHealthCheck },
      onSignal: container.resolve(SERVICES.ON_SIGNAL),
    });

    const osmdbtProcess = container.resolve<OsmdbtProcessor>(OSMDBT_PROCESSOR)();
    if (isSingleTask(osmdbtProcess)) {
      await osmdbtProcess();

      server.close(() => {
        logger.info({ msg: 'Server closed, exiting process' });
        process.exit(0);
      });

      setTimeout(() => {
        logger.error({ msg: 'Forcing shutdown...' });
        process.exit(1);
      }, FORCE_SHUTDOWN_TIMEOUT_MS);
    }

    server.listen(port, () => {
      logger.info({ msg: `app started on port ${port}` });
    });
  })
  .catch((error: Error) => {
    console.error('😢 - failed initializing the server');
    console.error(error);
    process.exit(1);
  });
