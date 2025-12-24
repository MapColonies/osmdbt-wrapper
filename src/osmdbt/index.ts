import { ScheduledTask, schedule as cronSchedule } from 'node-cron';
import { type Logger } from '@map-colonies/js-logger';
import { DependencyContainer, FactoryFunction } from 'tsyringe';
import { ConfigType } from '@src/common/config';
import { SERVICES } from '@src/common/constants';
import { AppConfig } from '@src/common/interfaces';
import { delay } from '@src/common/util';
import { OsmdbtService } from './osmdbtService';

type SingleTask = () => Promise<void>;

export type OsmdbtProcessor = ScheduledTask | SingleTask;

export const OSMDBT_PROCESSOR = Symbol('OsmdbtProcessor');

export function isSingleTask(value: ScheduledTask | SingleTask): value is SingleTask {
  return typeof value === 'function';
}

export const osmdbtProcessorFactory: FactoryFunction<OsmdbtProcessor> = (container: DependencyContainer) => {
  const logger = container.resolve<Logger>(SERVICES.LOGGER);
  const config = container.resolve<ConfigType>(SERVICES.CONFIG);
  const osmdbtService = container.resolve(OsmdbtService);

  const appConfig = config.get('app') as AppConfig;

  const isCron = appConfig.cron?.enabled;

  // single job
  if (isCron === undefined || !isCron) {
    logger.info({ msg: 'initializing osmdbt processor as a single job', isCron });
    const job = osmdbtService.executeJob.bind(osmdbtService);
    return job;
  }

  // cronjob
  const { failurePenalty, expression } = appConfig.cron!;

  logger.info({
    msg: 'initializing osmdbt processor as cronjob',
    isCron,
    cronExpression: expression,
    failurePenalty,
  });

  const scheduledTask = cronSchedule(
    expression,
    async () => {
      try {
        await osmdbtService.executeJob();
      } catch (error) {
        logger.error({ msg: 'osmdbt job execution errored', error, failurePenalty, expression });
        await delay(failurePenalty);
      }
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

  return scheduledTask;
};
