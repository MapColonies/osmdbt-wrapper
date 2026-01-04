import execa from 'execa';
import jsLogger from '@map-colonies/js-logger';
import { Registry } from 'prom-client';
import { ConfigType, getConfig, initConfig } from '@src/common/config';
import { ErrorWithExitCode } from '@src/common/errors';
import { ExitCodes } from '@src/common/constants';
import { OsmiumExecutable } from '@src/executables/osmium';

jest.mock('execa', () => ({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  __esModule: true,
  default: jest.fn(),
}));

const MOCK_PATH = '/mock/path';

describe('osmium', () => {
  const execaMock = execa as jest.MockedFunction<typeof execa>;

  let config: ConfigType;
  let osmiumExecutable: OsmiumExecutable;

  beforeAll(async () => {
    await initConfig(true);
    config = getConfig();
    osmiumExecutable = new OsmiumExecutable(jsLogger({ enabled: false }), config, new Registry());
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('executableName', () => {
    it('should return executable name', () => {
      expect(osmiumExecutable.executableName).toBe('osmium');
    });
  });

  describe('fileInfo', () => {
    it('should resolve and execute osmdbt get-log with given configuration', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 0, stdout: 'stdout', stderr: 'stderr' } as never);

      await expect(osmiumExecutable.fileInfo(MOCK_PATH)).resolves.not.toThrow();

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/fileinfo', ['--no-progress', '--extended', '--json', MOCK_PATH], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process exit code is not 0 and it has stderr', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 1, stdout: 'stdout', stderr: 'stderr' } as never);

      await expect(osmiumExecutable.fileInfo(MOCK_PATH)).rejects.toStrictEqual(new ErrorWithExitCode('stderr', ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/fileinfo', ['--no-progress', '--extended', '--json', MOCK_PATH], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process exit code is not 0 and it has no stderr', async () => {
      execaMock.mockResolvedValueOnce({ exitCode: 1, stdout: 'stdout', stderr: '' } as never);

      await expect(osmiumExecutable.fileInfo(MOCK_PATH)).rejects.toStrictEqual(
        new ErrorWithExitCode('osmium fileinfo failed with exit code 1', ExitCodes.OSMDBT_ERROR)
      );

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/fileinfo', ['--no-progress', '--extended', '--json', MOCK_PATH], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process throws an error', async () => {
      const error = new Error('child process error');
      execaMock.mockRejectedValueOnce(error);

      await expect(osmiumExecutable.fileInfo(MOCK_PATH)).rejects.toStrictEqual(new ErrorWithExitCode(error.message, ExitCodes.OSMDBT_ERROR));

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/fileinfo', ['--no-progress', '--extended', '--json', MOCK_PATH], {
        encoding: 'utf-8',
      });
    });

    it('should throw if executable process throws an error without message', async () => {
      const error = new Error();
      execaMock.mockRejectedValueOnce(error);

      await expect(osmiumExecutable.fileInfo(MOCK_PATH)).rejects.toStrictEqual(
        new ErrorWithExitCode('osmium fileinfo errored', ExitCodes.OSMDBT_ERROR)
      );

      expect(execaMock).toHaveBeenCalledTimes(1);
      expect(execaMock).toHaveBeenCalledWith('/osmdbt/build/src/fileinfo', ['--no-progress', '--extended', '--json', MOCK_PATH], {
        encoding: 'utf-8',
      });
    });
  });
});
