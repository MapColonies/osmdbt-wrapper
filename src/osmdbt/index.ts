import { ScheduledTask, schedule as cronSchedule } from 'node-cron';
import { type Logger } from '@map-colonies/js-logger';
import { DependencyContainer, FactoryFunction } from 'tsyringe';
import { ConfigType } from '@src/common/config';
import { SERVICES } from '@src/common/constants';
import { AppConfig } from '@src/common/interfaces';
import { tryCatch } from '@src/try-catch';
import { delay } from '@src/util';
import { OsmdbtService } from './osmdbtService';

type SingleTask = () => Promise<void>;

type OsmdbtProcessorFuncReturnType = Promise<ScheduledTask | SingleTask>;

let cachedProcessorResult: Awaited<OsmdbtProcessorFuncReturnType> | undefined = undefined;

export const OSMDBT_PROCESSOR = Symbol('OsmdbtProcessor');

export type OsmdbtProcessor = (preventInit?: boolean) => OsmdbtProcessorFuncReturnType;

export function isSingleTask(value: ScheduledTask | SingleTask): value is SingleTask {
  return typeof value === 'function';
}

export const osmdbtProcessorFactory: FactoryFunction<OsmdbtProcessor> = (container: DependencyContainer) => {
  const logger = container.resolve<Logger>(SERVICES.LOGGER);
  const config = container.resolve<ConfigType>(SERVICES.CONFIG);

  const appConfig = config.get('app') as AppConfig;
  const osmdbtService = container.resolve(OsmdbtService);

  const osmdbtProcessor: OsmdbtProcessor = async (preventInit = false): OsmdbtProcessorFuncReturnType => {
    if (cachedProcessorResult !== undefined) {
      return cachedProcessorResult;
    }

    if (preventInit) {
      throw new Error('OsmdbtProcessor has not been initialized yet. Please call it without preventInit first.');
    }

    const isCron = appConfig.cron?.enabled;

    if (!isCron) {
      logger.info({ msg: 'Run mode: Running osmdbt job once' });
      const job = osmdbtService.startJob.bind(osmdbtService);

      cachedProcessorResult = job;
      return job;
    }

    const { failurePenaltySeconds } = appConfig.cron!;

    logger.info({
      msg: 'Run mode: CronJob',
      cronExpression: appConfig.cron!.expression,
      failurePenaltySeconds: appConfig.cron!.failurePenaltySeconds,
    });

    const scheduledTask = cronSchedule(
      appConfig.cron!.expression,
      async () => {
        const res = await tryCatch(osmdbtService.startJob());

        if (res.error) {
          logger.error({ msg: 'Error during osmdbt job', error: res.error, failurePenaltySeconds });
          await delay(failurePenaltySeconds);
        }
      },
      {
        noOverlap: true,
      }
    );
    cachedProcessorResult = scheduledTask;
    return scheduledTask;
  };

  return osmdbtProcessor;
};
