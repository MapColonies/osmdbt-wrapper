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

type OsmdbtProcessorFuncReturnType = ScheduledTask | SingleTask;

let cachedProcessorResult: Awaited<OsmdbtProcessorFuncReturnType> | undefined = undefined;

export const OSMDBT_PROCESSOR = Symbol('OsmdbtProcessor');

export type OsmdbtProcessor = () => OsmdbtProcessorFuncReturnType;

export function isSingleTask(value: ScheduledTask | SingleTask): value is SingleTask {
  return typeof value === 'function';
}

export const osmdbtProcessorFactory: FactoryFunction<OsmdbtProcessor> = (container: DependencyContainer) => {
  const logger = container.resolve<Logger>(SERVICES.LOGGER);
  const config = container.resolve<ConfigType>(SERVICES.CONFIG);

  const appConfig = config.get('app') as AppConfig;
  const osmdbtService = container.resolve(OsmdbtService);

  const osmdbtProcessor: OsmdbtProcessor = (): OsmdbtProcessorFuncReturnType => {
    if (cachedProcessorResult !== undefined) {
      return cachedProcessorResult;
    }

    const isCron = appConfig.cron?.enabled;

    if (isCron === undefined || !isCron) {
      logger.info({ msg: 'initializing osmdbt processor', isCron });
      const job = osmdbtService.startJob.bind(osmdbtService);

      cachedProcessorResult = job;
      return job;
    }

    const { failurePenalty, expression } = appConfig.cron!;

    logger.info({
      msg: 'initializing osmdbt processor',
      isCron,
      cronExpression: expression,
      failurePenalty,
    });

    const scheduledTask = cronSchedule(
      expression,
      async () => {
        const { error } = await tryCatch(osmdbtService.startJob());

        if (error) {
          logger.error({ msg: 'error during osmdbt job', error, failurePenalty, expression });
          await delay(failurePenalty);
        }

        return;
      },
      {
        noOverlap: true,
      }
    );

    scheduledTask.on('task:started', (ctx) => logger.debug({ msg: '>>> fired task:started event', ctx }));
    scheduledTask.on('task:stopped', (ctx) => logger.debug({ msg: '>>> fired task:stopped event', ctx }));
    scheduledTask.on('task:destroyed', (ctx) => logger.debug({ msg: '>>> fired task:destroyed event', ctx }));
    scheduledTask.on('execution:started', (ctx) => logger.debug({ msg: '>>> fired execution:started event', ctx }));
    scheduledTask.on('execution:failed', (ctx) => logger.debug({ msg: '>>> fired execution:failed event', ctx }));
    scheduledTask.on('execution:finished', (ctx) => logger.debug({ msg: '>>> fired execution:finished event', ctx }));
    scheduledTask.on('execution:missed', (ctx) => logger.warn({ msg: '>>> fired execution:missed event', ctx }));
    scheduledTask.on('execution:overlap', (ctx) => logger.warn({ msg: '>>> fired execution:overlap event', ctx }));

    cachedProcessorResult = scheduledTask;

    return scheduledTask;
  };

  return osmdbtProcessor;
};
