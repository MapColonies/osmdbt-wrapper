import { ScheduledTask, schedule as cronSchedule } from 'node-cron';
import { type Logger } from '@map-colonies/js-logger';
import { DependencyContainer, FactoryFunction } from 'tsyringe';
import { ConfigType } from '@src/common/config';
import { MILLISECONDS_IN_SECOND, SERVICES } from '@src/common/constants';
import { AppConfig } from '@src/common/interfaces';
import { tryCatch } from '@src/try-catch';
import { delay } from '@src/util';
import { OsmdbtService } from './osmdbtService';

type OsmdbtProcessorFuncReturnType = Promise<ScheduledTask | void>;

let cachedProcessorResult: Awaited<OsmdbtProcessorFuncReturnType> | undefined = undefined;

export const OSMDBT_PROCESSOR = Symbol('OsmdbtProcessor');

export type OsmdbtProcessor = () => OsmdbtProcessorFuncReturnType;

export const osmdbtProcessorFactory: FactoryFunction<OsmdbtProcessor> = (container: DependencyContainer) => {
  const logger = container.resolve<Logger>(SERVICES.LOGGER);
  const config = container.resolve<ConfigType>(SERVICES.CONFIG);

  const appConfig = config.get('app') as AppConfig;
  const osmdbtService = container.resolve(OsmdbtService);

  const osmdbtProcessor: OsmdbtProcessor = async (): OsmdbtProcessorFuncReturnType => {
    if (cachedProcessorResult !== undefined) {
      return cachedProcessorResult;
    }

    const runFn = async (failurePenalty: number = 0): Promise<void> => {
      logger.info({ msg: 'Starting osmdbt job' });
      const res = await tryCatch(osmdbtService.startJob());
      if (res.error) {
        failurePenalty *= MILLISECONDS_IN_SECOND;
        logger.error({ msg: 'Error during osmdbt job', error: res.error, failurePenalty });
        await delay(failurePenalty);
      }
      logger.info({ msg: 'Finished osmdbt job' });
      return;
    };

    if (appConfig.cron?.enabled === true) {
      const { failurePenaltySeconds } = appConfig.cron;

      logger.info({
        msg: 'Run mode: CronJob',
        cronExpression: appConfig.cron.expression,
        failurePenaltySeconds: appConfig.cron.failurePenaltySeconds,
      });

      const scheduledTask = cronSchedule(
        appConfig.cron.expression,
        async () => {
          await runFn(failurePenaltySeconds);
        },
        {
          noOverlap: true,
        }
      );
      cachedProcessorResult = scheduledTask;
      return scheduledTask;
    } else {
      logger.info({ msg: 'Run mode: Running osmdbt job once' });
      await runFn();
      return cachedProcessorResult;
    }
  };

  return osmdbtProcessor;
};
