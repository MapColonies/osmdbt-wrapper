import { join } from 'path';
import { Registry as PromRegistry } from 'prom-client';
import { type Logger } from '@map-colonies/js-logger';
import { inject, injectable } from 'tsyringe';
import { GLOBAL_OSMDBT_NON_VERBOSE_ARGS, GLOBAL_OSMDBT_VERBOSE_ARGS, OSMDBT_BIN_PATH, SERVICES } from '@src/common/constants';
import { type ConfigType } from '@src/common/config';
import { OsmdbtConfig } from '@src/common/interfaces';
import { CommonExecutable } from './common';
import { CommandContext, Executable, OsmdbtCommand } from '.';

@injectable()
export class OsmdbtExecutable extends CommonExecutable {
  private readonly osmdbtConfig: OsmdbtConfig;

  public constructor(
    @inject(SERVICES.LOGGER) logger: Logger,
    @inject(SERVICES.CONFIG) config: ConfigType,
    @inject(SERVICES.METRICS) registry?: PromRegistry
  ) {
    super(logger, config, registry);

    this.osmdbtConfig = config.get('osmdbt') as OsmdbtConfig;
  }

  public getExecutableName(): Executable {
    return 'osmdbt';
  }

  public async getLog(): Promise<void> {
    const context = { executable: this.executableName, command: OsmdbtCommand.GET_LOG, args: ['-m', this.osmdbtConfig.getLogMaxChanges.toString()] };
    await this.runCommand(context);
  }

  public async createDiff(): Promise<void> {
    const context = { executable: this.executableName, command: OsmdbtCommand.CREATE_DIFF, args: [] };
    await this.runCommand(context);
  }

  public async catchup(): Promise<void> {
    const context = { executable: this.executableName, command: OsmdbtCommand.CATCHUP, args: [] };
    await this.runCommand(context);
  }

  protected getExecutablePath(context: CommandContext): string {
    return join(OSMDBT_BIN_PATH, context.command);
  }

  protected getArgs(context: CommandContext): string[] {
    return [...this.getGlobalArgs(), ...context.args];
  }

  protected getGlobalArgs(): string[] {
    return this.osmdbtConfig.verbose ? GLOBAL_OSMDBT_VERBOSE_ARGS : GLOBAL_OSMDBT_NON_VERBOSE_ARGS;
  }
}
