import http from 'http';
import { createTerminus } from '@godaddy/terminus';
import { FactoryFunction } from 'tsyringe';
import { ON_SIGNAL } from './constants';

const stubHealthcheck = async (): Promise<void> => Promise.resolve();

export type TerminusFactory = (server: http.Server) => http.Server;

export const terminusFactory: FactoryFunction<TerminusFactory> = (container) => {
  return (server: http.Server): http.Server => {
    return createTerminus(server, {
      healthChecks: { '/liveness': stubHealthcheck },
      onSignal: container.resolve(ON_SIGNAL),
    });
  };
};
