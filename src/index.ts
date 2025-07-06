// this import must be called before the first import of tsyringe
import 'reflect-metadata';
import { createServer } from 'http';
import { createTerminus } from '@godaddy/terminus';
import { Logger } from '@map-colonies/js-logger';
import { SERVICES } from '@common/constants';
import { ConfigType } from '@common/config';
import { getApp } from './app';
import { OSMDBT_PROCESSOR, OsmdbtProcessor } from './osmdbt';

const FORCE_SHUTDOWN_TIMEOUT = 10000; // 10 seconds

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

    const osmdbtProcess = await container.resolve<OsmdbtProcessor>(OSMDBT_PROCESSOR)();
    if (typeof osmdbtProcess !== 'object') {
      server.close(() => {
        logger.info('Server closed, exiting process');
        process.exit(0);
      });

      setTimeout(() => {
        console.error('Forcing shutdown...');
        process.exit(1);
      }, FORCE_SHUTDOWN_TIMEOUT);
    }

    server.listen(port, () => {
      logger.info(`app started on port ${port}`);
    });
  })
  .catch((error: Error) => {
    console.error('😢 - failed initializing the server');
    console.error(error);
    process.exit(1);
  });
