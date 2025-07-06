import { ScheduledTask, schedule as cronSchedule } from 'node-cron';
import { type Logger } from '@map-colonies/js-logger';
import { DependencyContainer, FactoryFunction } from 'tsyringe';
import { ConfigType } from '@src/common/config';
import { SERVICES } from '@src/common/constants';
import { AppConfig } from '@src/common/interfaces';
import { tryCatch } from '@src/try-catch';
import { OsmdbtService } from './osmdbtService';

type OsmdbtProcessorFuncReturnType = Promise<ScheduledTask | void>;

let osmdbtProcessor: OsmdbtProcessor | undefined = undefined;

const MILLISECONDS_IN_SECOND = 1000;

export const OSMDBT_PROCESSOR = Symbol('OsmdbtProcessor');

export type OsmdbtProcessor = () => OsmdbtProcessorFuncReturnType;

export const osmdbtProcessorFactory: FactoryFunction<OsmdbtProcessor> = (container: DependencyContainer) => {
  if (osmdbtProcessor !== undefined) {
    return osmdbtProcessor;
  }
  const logger = container.resolve<Logger>(SERVICES.LOGGER);
  const config = container.resolve<ConfigType>(SERVICES.CONFIG);

  const appConfig = config.get('app') as AppConfig;
  const osmdbtService = container.resolve(OsmdbtService);

  osmdbtProcessor = async (): OsmdbtProcessorFuncReturnType => {
    const runFn = async (failurePenalty: number = 0): Promise<void> => {
      logger.info('Starting osmdbt job');
      const res = await tryCatch(osmdbtService.startJob());
      if (res.error) {
        logger.error('Error during osmdbt job', { error: res.error });
        await new Promise((resolve) => setTimeout(resolve, failurePenalty * MILLISECONDS_IN_SECOND));
        throw res.error;
      }
      logger.info('Finished osmdbt job');
      return;
    };

    if (appConfig.cron?.enabled === true) {
      const { failurePenaltySeconds } = appConfig.cron;
      const scheduledTask = cronSchedule(
        appConfig.cron.expression,
        async () => {
          await runFn(failurePenaltySeconds);
        },
        {
          noOverlap: false,
        }
      );

      return scheduledTask;
    } else {
      await runFn();
    }
  };

  return osmdbtProcessor;
};
