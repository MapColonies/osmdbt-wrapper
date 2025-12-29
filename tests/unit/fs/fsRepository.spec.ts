import * as fsPromises from 'fs/promises';
import jsLogger from '@map-colonies/js-logger';
import { initConfig } from '@src/common/config';
import { FsRepository } from '@src/fs/fsRepository';
import { ErrorWithExitCode } from '@src/common/errors';
import { ExitCodes } from '@src/common/constants';

jest.mock('fs/promises');

const MOCK_DIR_PATH = '/mock/path';
const MOCK_FILE_PATH = '/mock/path/file.txt';

let fsRepository: FsRepository;

describe('fsRepository', () => {
  beforeAll(async () => {
    await initConfig(true);
  });

  beforeEach(() => {
    fsRepository = new FsRepository(jsLogger({ enabled: false }));
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('readFile', () => {
    it('should run successfully', async () => {
      const expected = Buffer.from('lorem ipsum');
      (fsPromises.readFile as jest.Mock).mockResolvedValue(expected);

      await expect(fsRepository.readFile(MOCK_FILE_PATH, 'utf8')).resolves.toBe(expected);

      expect(fsPromises.readFile).toHaveBeenCalledTimes(1);
      expect(fsPromises.readFile).toHaveBeenCalledWith(MOCK_FILE_PATH, 'utf8');
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');

      (fsPromises.readFile as jest.Mock).mockRejectedValueOnce(error);

      await expect(fsRepository.readFile(MOCK_FILE_PATH)).rejects.toStrictEqual(new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR));
      expect(fsPromises.readFile).toHaveBeenCalledTimes(1);
      expect(fsPromises.readFile).toHaveBeenCalledWith(MOCK_FILE_PATH, undefined);
    });
  });

  describe('mkdir', () => {
    it('should run successfully', async () => {
      (fsPromises.mkdir as jest.Mock).mockResolvedValue(MOCK_DIR_PATH);

      await expect(fsRepository.mkdir(MOCK_DIR_PATH)).resolves.toBe(MOCK_DIR_PATH);

      expect(fsPromises.mkdir).toHaveBeenCalledTimes(1);
      expect(fsPromises.mkdir).toHaveBeenCalledWith(MOCK_DIR_PATH, { recursive: true });
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');

      (fsPromises.mkdir as jest.Mock).mockRejectedValueOnce(error);

      await expect(fsRepository.mkdir(MOCK_FILE_PATH)).rejects.toStrictEqual(new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR));
      expect(fsPromises.mkdir).toHaveBeenCalledTimes(1);
      expect(fsPromises.mkdir).toHaveBeenCalledWith(MOCK_FILE_PATH, { recursive: true });
    });
  });

  describe('appendFile', () => {
    const mockData = 'mock data';
    it('should run successfully', async () => {
      (fsPromises.appendFile as jest.Mock).mockResolvedValue(undefined);

      await expect(fsRepository.appendFile(MOCK_FILE_PATH, mockData, 'utf-8')).resolves.toBeUndefined();

      expect(fsPromises.appendFile).toHaveBeenCalledTimes(1);
      expect(fsPromises.appendFile).toHaveBeenCalledWith(MOCK_FILE_PATH, mockData, 'utf-8');
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');
      (fsPromises.appendFile as jest.Mock).mockRejectedValueOnce(error);

      await expect(fsRepository.appendFile(MOCK_FILE_PATH, mockData, 'utf-8')).rejects.toStrictEqual(
        new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR)
      );

      expect(fsPromises.appendFile).toHaveBeenCalledTimes(1);
      expect(fsPromises.appendFile).toHaveBeenCalledWith(MOCK_FILE_PATH, mockData, 'utf-8');
    });
  });

  describe('readdir', () => {
    it('should run successfully', async () => {
      const expected = ['file1.txt', 'file2.txt'];
      (fsPromises.readdir as jest.Mock).mockResolvedValue(expected);

      await expect(fsRepository.readdir(MOCK_DIR_PATH)).resolves.toEqual(expected);

      expect(fsPromises.readdir).toHaveBeenCalledTimes(1);
      expect(fsPromises.readdir).toHaveBeenCalledWith(MOCK_DIR_PATH);
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');
      (fsPromises.readdir as jest.Mock).mockRejectedValueOnce(error);

      await expect(fsRepository.readdir(MOCK_DIR_PATH)).rejects.toStrictEqual(new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR));

      expect(fsPromises.readdir).toHaveBeenCalledTimes(1);
      expect(fsPromises.readdir).toHaveBeenCalledWith(MOCK_DIR_PATH);
    });
  });

  describe('rename', () => {
    it('should run successfully', async () => {
      const renamedFilePath = '/mock/renamed/file.txt';
      (fsPromises.rename as jest.Mock).mockResolvedValue(undefined);

      await expect(fsRepository.rename(MOCK_FILE_PATH, renamedFilePath)).resolves.toBeUndefined();

      expect(fsPromises.rename).toHaveBeenCalledTimes(1);
      expect(fsPromises.rename).toHaveBeenCalledWith(MOCK_FILE_PATH, renamedFilePath);
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');
      (fsPromises.rename as jest.Mock).mockRejectedValueOnce(error);

      await expect(fsRepository.rename(MOCK_FILE_PATH, MOCK_FILE_PATH)).rejects.toStrictEqual(new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR));

      expect(fsPromises.rename).toHaveBeenCalledTimes(1);
      expect(fsPromises.rename).toHaveBeenCalledWith(MOCK_FILE_PATH, MOCK_FILE_PATH);
    });
  });

  describe('unlink', () => {
    it('should run successfully', async () => {
      (fsPromises.unlink as jest.Mock).mockResolvedValue(undefined);

      await expect(fsRepository.unlink(MOCK_FILE_PATH)).resolves.toBeUndefined();

      expect(fsPromises.unlink).toHaveBeenCalledTimes(1);
      expect(fsPromises.unlink).toHaveBeenCalledWith(MOCK_FILE_PATH);
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');
      (fsPromises.unlink as jest.Mock).mockRejectedValueOnce(error);

      await expect(fsRepository.unlink(MOCK_FILE_PATH)).rejects.toStrictEqual(new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR));

      expect(fsPromises.unlink).toHaveBeenCalledTimes(1);
      expect(fsPromises.unlink).toHaveBeenCalledWith(MOCK_FILE_PATH);
    });
  });

  describe('writeFile', () => {
    it('should run successfully', async () => {
      const data = 'mock content';
      (fsPromises.writeFile as jest.Mock).mockResolvedValue(undefined);

      await expect(fsRepository.writeFile(MOCK_FILE_PATH, data)).resolves.toBeUndefined();

      expect(fsPromises.writeFile).toHaveBeenCalledTimes(1);
      expect(fsPromises.writeFile).toHaveBeenCalledWith(MOCK_FILE_PATH, data, undefined);
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const data = 'mock content';
      const error = new Error('some error');
      (fsPromises.writeFile as jest.Mock).mockRejectedValueOnce(error);

      await expect(fsRepository.writeFile(MOCK_FILE_PATH, data)).rejects.toStrictEqual(new ErrorWithExitCode('fs error', ExitCodes.FS_ERROR));

      expect(fsPromises.writeFile).toHaveBeenCalledTimes(1);
      expect(fsPromises.writeFile).toHaveBeenCalledWith(MOCK_FILE_PATH, data, undefined);
    });
  });
});
