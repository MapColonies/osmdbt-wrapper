import { DependencyContainer, FactoryFunction } from 'tsyringe';
import { StatefulMediator } from '@map-colonies/arstotzka-mediator';
import { type Logger } from '@map-colonies/js-logger';
import { ConfigType } from '@src/common/config';
import { SERVICES } from '@src/common/constants';
import { ArstotzkaConfig } from '@src/common/interfaces';

export type Mediator = Pick<StatefulMediator, 'createAction' | 'removeLock' | 'reserveAccess' | 'updateAction'>;

export const mediatorFactory: FactoryFunction<Mediator> = (container: DependencyContainer) => {
  const config = container.resolve<ConfigType>(SERVICES.CONFIG);
  const logger = container.resolve<Logger>(SERVICES.LOGGER);

  const arstotzkaConfig = config.get('arstotzka') as ArstotzkaConfig;

  let mediator: StatefulMediator | undefined;

  if (arstotzkaConfig.enabled) {
    mediator = new StatefulMediator({ ...arstotzkaConfig.mediator, serviceId: arstotzkaConfig.serviceId, logger });
  } else {
    const msg = 'Mediator is not enabled, but it is required for the application to run';
    logger.fatal({ msg });
    throw new Error(msg);
  }

  return {
    async reserveAccess(...args: Parameters<Mediator['reserveAccess']>): ReturnType<Mediator['reserveAccess']> {
      return mediator.reserveAccess(...args);
    },
    async removeLock(...args: Parameters<Mediator['removeLock']>): ReturnType<Mediator['removeLock']> {
      return mediator.removeLock(...args);
    },
    async createAction(...args: Parameters<Mediator['createAction']>): Promise<Awaited<ReturnType<Mediator['createAction']>>> {
      return mediator.createAction(...args);
    },
    async updateAction(...args: Parameters<Mediator['updateAction']>): ReturnType<Mediator['updateAction']> {
      return mediator.updateAction(...args);
    },
  };
};
