import execa from 'execa';
import jsLogger from '@map-colonies/js-logger';
import { Registry } from 'prom-client';
import { ConfigType, getConfig, initConfig } from '@src/common/config';
import { OsmdbtExecutable } from '@src/executables/osmdbt';
import { ErrorWithExitCode } from '@src/common/errors';
import { ExitCodes } from '@src/common/constants';

jest.mock('execa', () => ({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  __esModule: true,
  default: jest.fn(),
}));

describe('osmdbt', () => {
  const execaMock = execa as jest.MockedFunction<typeof execa>;

  let config: ConfigType;
  let osmdbtExecutable: OsmdbtExecutable;

  beforeAll(async () => {
    await initConfig(true);
    config = getConfig();
    osmdbtExecutable = new OsmdbtExecutable(jsLogger({ enabled: false }), config, new Registry());
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('executableName', () => {
    it('should return executable name', () => {
      expect(osmdbtExecutable.executableName).toBe('osmdbt');
    });
  });

  describe('getLog', () => {
    it('should resolve and execute osmdbt get-log with given configuration', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 0, stdout: 'stdout', stderr: 'stderr' } as never);

      await expect(osmdbtExecutable.getLog()).resolves.not.toThrow();

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith(
        '/osmdbt/build/src/osmdbt-get-log',
        ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q', '-m', config.get('osmdbt')?.getLogMaxChanges.toString()],
        { encoding: 'utf-8' }
      );
    });

    it('should throw if executable process exit code is not 0 and it has stderr', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 1, stdout: 'stdout', stderr: 'stderr' } as never);

      await expect(osmdbtExecutable.getLog()).rejects.toStrictEqual(new ErrorWithExitCode('stderr', ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith(
        '/osmdbt/build/src/osmdbt-get-log',
        ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q', '-m', config.get('osmdbt')?.getLogMaxChanges.toString()],
        { encoding: 'utf-8' }
      );
    });

    it('should throw if executable process exit code is not 0 and it has no stderr', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 1, stdout: 'stdout', stderr: '' } as never);

      await expect(osmdbtExecutable.getLog()).rejects.toStrictEqual(
        new ErrorWithExitCode('osmdbt osmdbt-get-log failed with exit code 1', ExitCodes.OSMDBT_ERROR)
      );

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith(
        '/osmdbt/build/src/osmdbt-get-log',
        ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q', '-m', config.get('osmdbt')?.getLogMaxChanges.toString()],
        { encoding: 'utf-8' }
      );
    });

    it('should throw if executable process throws an error', async () => {
      const error = new Error('child process error');
      execaMock.mockRejectedValueOnce(error);

      await expect(osmdbtExecutable.getLog()).rejects.toStrictEqual(new ErrorWithExitCode(error.message, ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith(
        '/osmdbt/build/src/osmdbt-get-log',
        ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q', '-m', config.get('osmdbt')?.getLogMaxChanges.toString()],
        { encoding: 'utf-8' }
      );
    });

    it('should throw if executable process throws an error without message', async () => {
      const error = new Error();
      execaMock.mockRejectedValueOnce(error);

      await expect(osmdbtExecutable.getLog()).rejects.toStrictEqual(new ErrorWithExitCode('osmdbt osmdbt-get-log errored', ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith(
        '/osmdbt/build/src/osmdbt-get-log',
        ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q', '-m', config.get('osmdbt')?.getLogMaxChanges.toString()],
        { encoding: 'utf-8' }
      );
    });
  });

  describe('createDiff', () => {
    it('should resolve and execute osmdbt create-diff with given configuration', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 0, stdout: 'stdout', stderr: 'stderr' } as never);

      await expect(osmdbtExecutable.createDiff()).resolves.not.toThrow();

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-create-diff', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process exit code is not 0 and it has stderr', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 1, stdout: 'stdout', stderr: 'stderr' } as never);

      await expect(osmdbtExecutable.createDiff()).rejects.toStrictEqual(new ErrorWithExitCode('stderr', ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-create-diff', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process exit code is not 0 and it has no stderr', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 1, stdout: 'stdout', stderr: '' } as never);

      await expect(osmdbtExecutable.createDiff()).rejects.toStrictEqual(
        new ErrorWithExitCode('osmdbt osmdbt-create-diff failed with exit code 1', ExitCodes.OSMDBT_ERROR)
      );

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-create-diff', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process throws an error', async () => {
      const error = new Error('child process error');
      execaMock.mockRejectedValueOnce(error);

      await expect(osmdbtExecutable.createDiff()).rejects.toStrictEqual(new ErrorWithExitCode(error.message, ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-create-diff', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process throws an error without message', async () => {
      const error = new Error();
      execaMock.mockRejectedValueOnce(error);

      await expect(osmdbtExecutable.createDiff()).rejects.toStrictEqual(
        new ErrorWithExitCode('osmdbt osmdbt-create-diff errored', ExitCodes.OSMDBT_ERROR)
      );

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-create-diff', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });
  });

  describe('catchup', () => {
    it('should resolve and execute osmdbt create-diff with given configuration', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 0, stdout: 'stdout', stderr: 'stderr' } as never);

      await expect(osmdbtExecutable.catchup()).resolves.not.toThrow();

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-catchup', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process exit code is not 0 and it has stderr', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 1, stdout: 'stdout', stderr: 'stderr' } as never);

      await expect(osmdbtExecutable.catchup()).rejects.toStrictEqual(new ErrorWithExitCode('stderr', ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-catchup', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process exit code is not 0 and it has no stderr', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 1, stdout: 'stdout', stderr: '' } as never);

      await expect(osmdbtExecutable.catchup()).rejects.toStrictEqual(
        new ErrorWithExitCode('osmdbt osmdbt-catchup failed with exit code 1', ExitCodes.OSMDBT_ERROR)
      );

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-catchup', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process throws an error', async () => {
      const error = new Error('child process error');
      execaMock.mockRejectedValueOnce(error);

      await expect(osmdbtExecutable.catchup()).rejects.toStrictEqual(new ErrorWithExitCode(error.message, ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-catchup', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process throws an error without message', async () => {
      const error = new Error();
      execaMock.mockRejectedValueOnce(error);

      await expect(osmdbtExecutable.catchup()).rejects.toStrictEqual(new ErrorWithExitCode('osmdbt osmdbt-catchup errored', ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/osmdbt-catchup', ['-c', '/osmdbt/config/osmdbt-config.yaml', '-q'], {
        encoding: 'utf-8',
      });
    });
  });
});
