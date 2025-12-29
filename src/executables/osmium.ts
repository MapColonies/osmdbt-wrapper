import { join } from 'path';
import { Registry as PromRegistry } from 'prom-client';
import { type Logger } from '@map-colonies/js-logger';
import { inject, injectable } from 'tsyringe';
import { OSMDBT_BIN_PATH, SERVICES } from '@src/common/constants';
import { type ConfigType } from '@src/common/config';
import { OsmiumConfig } from '@src/common/interfaces';
import { CommonExecutable } from './common';
import { CommandContext, Executable, OsmiumCommand } from '.';

@injectable()
export class OsmiumExecutable extends CommonExecutable {
  private readonly osmiumConfig: OsmiumConfig;

  public constructor(
    @inject(SERVICES.LOGGER) logger: Logger,
    @inject(SERVICES.CONFIG) config: ConfigType,
    @inject(SERVICES.METRICS) registry?: PromRegistry
  ) {
    super(logger, config, registry);

    this.osmiumConfig = config.get('osmium') as OsmiumConfig;
  }

  public getExecutableName(): Executable {
    return 'osmium';
  }

  public async fileInfo(diffPath: string): Promise<string> {
    const context = { executable: this.executableName, command: OsmiumCommand.FILE_INFO, args: ['--extended', '--json', diffPath] };

    const info = await this.runCommand(context);

    return info;
  }

  protected getExecutablePath(context: CommandContext): string {
    return join(OSMDBT_BIN_PATH, context.command);
  }

  protected getArgs(context: CommandContext): string[] {
    return [...this.getGlobalArgs(), ...context.args];
  }

  protected getGlobalArgs(): string[] {
    const progressFlag = this.osmiumConfig.progress ? '--progress' : '--no-progress';
    return this.osmiumConfig.verbose ? ['--verbose', progressFlag] : [progressFlag];
  }
}
