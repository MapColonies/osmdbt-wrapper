import * as fsPromises from 'fs/promises';
import { type Dirent } from 'fs';
import jsLogger from '@map-colonies/js-logger';
import { initConfig } from '@src/common/config';
import { FsRepository } from '@src/fs/fsRepository';

jest.mock('fs/promises');

//,

let fsRepository: FsRepository;
describe('fsRepository', () => {
  beforeAll(async () => {
    await initConfig(true);
  });

  beforeEach(() => {
    fsRepository = new FsRepository(jsLogger({ enabled: false }));
  });

  describe('readFile', () => {
    const mockReadFile = fsPromises.readFile as jest.MockedFunction<typeof fsPromises.readFile>;
    const mockPath = '/mock/path/file.txt';

    it('should run successfully', async () => {
      const mockData = Buffer.from('lorem ipsum');
      mockReadFile.mockResolvedValue(mockData);

      await expect(fsRepository.readFile(mockPath)).resolves.toBe(mockData);

      expect(mockReadFile).toHaveBeenCalledTimes(1);
      mockReadFile.mockReset();
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');

      mockReadFile.mockRejectedValueOnce(error);

      await expect(fsRepository.readFile(mockPath)).rejects.toBe(error);
      mockReadFile.mockReset();
    });
  });

  describe('mkdir', () => {
    const mockMkdir = fsPromises.mkdir as jest.MockedFunction<typeof fsPromises.mkdir>;

    const mockPath = '/mock/path';
    it('should run successfully', async () => {
      mockMkdir.mockResolvedValue(mockPath);

      await expect(fsRepository.mkdir(mockPath)).resolves.toBe(mockPath);

      expect(mockMkdir).toHaveBeenCalledTimes(1);
      mockMkdir.mockReset();
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');

      mockMkdir.mockRejectedValueOnce(error);

      await expect(fsRepository.mkdir(mockPath)).rejects.toBe(error);
      mockMkdir.mockReset();
    });
  });

  describe('appendFile', () => {
    const mockAppendFile = fsPromises.appendFile as jest.MockedFunction<typeof fsPromises.appendFile>;

    const mockPath = '/mock/path';
    const mockData = 'mock data';
    it('should run successfully', async () => {
      mockAppendFile.mockResolvedValue(undefined);

      await expect(fsRepository.appendFile(mockPath, mockData, 'utf-8')).resolves.toBeUndefined();

      expect(mockAppendFile).toHaveBeenCalledTimes(1);
      mockAppendFile.mockReset();
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');

      mockAppendFile.mockRejectedValueOnce(error);

      await expect(fsRepository.appendFile(mockPath, mockData, 'utf-8')).rejects.toBe(error);
      mockAppendFile.mockReset();
    });
  });

  describe('readdir', () => {
    const mockReaddir = fsPromises.readdir as jest.MockedFunction<typeof fsPromises.readdir>;

    const mockPath = '/mock/path';
    const mockData = ['file1.txt', 'file2.txt'];
    it('should run successfully', async () => {
      mockReaddir.mockResolvedValue(mockData as unknown as Dirent[]);

      await expect(fsRepository.readdir(mockPath)).resolves.toEqual(mockData);

      expect(mockReaddir).toHaveBeenCalledTimes(1);
      mockReaddir.mockReset();
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');

      mockReaddir.mockRejectedValueOnce(error);

      await expect(fsRepository.readdir(mockPath)).rejects.toEqual(error);
      mockReaddir.mockReset();
    });
  });

  describe('rename', () => {
    const mockRename = fsPromises.rename as jest.MockedFunction<typeof fsPromises.rename>;

    const mockFirstPath = '/mock/first/path';
    const mockSecondPath = '/mock/second/path';
    it('should run successfully', async () => {
      mockRename.mockResolvedValue(undefined);

      await expect(fsRepository.rename(mockFirstPath, mockSecondPath)).resolves.toBeUndefined();

      expect(mockRename).toHaveBeenCalledTimes(1);
      mockRename.mockReset();
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');

      mockRename.mockRejectedValueOnce(error);

      await expect(fsRepository.rename(mockFirstPath, mockSecondPath)).rejects.toEqual(error);
      mockRename.mockReset();
    });
  });

  describe('unlink', () => {
    const mockUnlink = fsPromises.unlink as jest.MockedFunction<typeof fsPromises.unlink>;

    const mockPath = '/mock/path';
    it('should run successfully', async () => {
      mockUnlink.mockResolvedValue(undefined);

      await expect(fsRepository.unlink(mockPath)).resolves.toBeUndefined();

      expect(mockUnlink).toHaveBeenCalledTimes(1);
      mockUnlink.mockReset();
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');

      mockUnlink.mockRejectedValueOnce(error);

      await expect(fsRepository.unlink(mockPath)).rejects.toEqual(error);
      mockUnlink.mockReset();
    });
  });

  describe('writeFile', () => {
    const mockWriteFile = fsPromises.writeFile as jest.MockedFunction<typeof fsPromises.writeFile>;

    it('should run successfully', async () => {
      mockWriteFile.mockResolvedValue(undefined);

      await expect(fsRepository.writeFile('/mock/path/file.txt', 'mock content')).resolves.toBeUndefined();

      expect(mockWriteFile).toHaveBeenCalledTimes(1);
      mockWriteFile.mockReset();
    });

    it('should fail becuase fs.writeFile throws error', async () => {
      const error = new Error('some error');

      mockWriteFile.mockRejectedValueOnce(error);

      await expect(fsRepository.writeFile('/mock/path/file.txt', 'mock content')).rejects.toBe(error);
      mockWriteFile.mockReset();
    });
  });
});
