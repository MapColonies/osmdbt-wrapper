import { Histogram, Registry as PromRegistry } from 'prom-client';
import { type Logger } from '@map-colonies/js-logger';
import { inject } from 'tsyringe';
import execa from 'execa';
import { context as contextAPI } from '@opentelemetry/api';
import { ATTR_RPC_SYSTEM } from '@opentelemetry/semantic-conventions/incubating';
import { ErrorWithExitCode } from '@src/common/errors';
import { ExitCodes, SERVICES } from '@src/common/constants';
import { type ConfigType } from '@src/common/config';
import { MetricsConfig } from '@src/common/interfaces';
import { startActivePromisifiedSpan } from '@src/common/tracing/util';
import { commandToSpanName, ExecutableAttributes } from '@src/common/tracing/executable';
import { CommandContext, Executable } from '.';

export abstract class CommonExecutable {
  private readonly commandDurationHistogram?: Histogram;

  public constructor(
    @inject(SERVICES.LOGGER) private readonly logger: Logger,
    @inject(SERVICES.CONFIG) config: ConfigType,
    @inject(SERVICES.METRICS) registry?: PromRegistry
  ) {
    if (registry !== undefined) {
      const { osmdbtCommandDurationSeconds } = (config.get('telemetry.metrics') as MetricsConfig).buckets;

      this.commandDurationHistogram = new Histogram({
        name: `osmdbt_${this.executableName}_command_duration_seconds`,
        help: 'Duration of individual executable commands in seconds',
        labelNames: ['command', 'exitCode'] as const,
        buckets: osmdbtCommandDurationSeconds,
        registers: [registry],
      });
    }
  }

  public get executableName(): Executable {
    return this.getExecutableName();
  }

  protected async runCommand(context: CommandContext): Promise<string> {
    const spanName = commandToSpanName(context.command);

    return startActivePromisifiedSpan(
      spanName,
      {
        [ATTR_RPC_SYSTEM]: this.executableName,
        [ExecutableAttributes.EXECUTABLE_COMMAND]: context.command,
        [ExecutableAttributes.EXECUTABLE_COMMAND_ARGS]: context.args.join(' '),
      },
      contextAPI.active(),
      async () => this.execute(context)
    );
  }

  private async execute(context: CommandContext): Promise<string> {
    const { command } = context;
    const commandArgs = this.getArgs(context);
    const executablePath = this.getExecutablePath(context);

    this.logger.info({ msg: 'executing command', executable: this.executableName, executablePath, command, args: commandArgs });

    let exitCode: number = ExitCodes.SUCCESS;
    let commandTimer: ReturnType<Histogram['startTimer']> | undefined;

    try {
      commandTimer = this.commandDurationHistogram?.startTimer({ command });

      const spawnedChild = execa(executablePath, commandArgs, { encoding: 'utf-8' });

      const { exitCode: commandExitCode, stderr, stdout } = await spawnedChild;
      exitCode = commandExitCode;

      if (exitCode !== 0) {
        throw new ErrorWithExitCode(
          stderr.length > 0 ? stderr : `${this.executableName} ${command} failed with exit code ${exitCode}`,
          ExitCodes.OSMDBT_ERROR
        );
      }

      return stdout;
    } catch (error) {
      this.logger.error({ msg: 'failure occurred during command execution', executable: this.executableName, command, args: commandArgs });

      const message = error instanceof Error && error.message.length > 0 ? error.message : `${this.executableName} ${command} errored`;
      throw new ErrorWithExitCode(message, ExitCodes.OSMDBT_ERROR);
    } finally {
      commandTimer?.({ exitCode: exitCode });
    }
  }

  protected abstract getExecutableName(): Executable;

  protected abstract getArgs(context: CommandContext): string[];

  protected abstract getGlobalArgs(): string[];

  protected abstract getExecutablePath(context?: CommandContext): string;
}
