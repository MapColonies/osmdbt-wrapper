import { StatefulMediator } from '@map-colonies/arstotzka-mediator';
import { OsmdbtExecutable } from '@src/executables/osmdbt';
import { OsmiumExecutable } from '@src/executables/osmium';
import { FsRepository } from '@src/fs/fsRepository';
import { S3Manager } from '@src/s3/s3Manager';

export const s3ManagerMockFn = {
  getObjectMock: jest.fn(),
  putObjectMock: jest.fn(),
};

export const s3ManagerMock = {
  getObject: s3ManagerMockFn.getObjectMock,
  putObject: s3ManagerMockFn.putObjectMock,
} as unknown as S3Manager;

export const fsRepositoryMockFn = {
  readFileMock: jest.fn(),
  mkdirMock: jest.fn(),
  appendFileMock: jest.fn(),
  readdirMock: jest.fn(),
  renameMock: jest.fn(),
  unlinkMock: jest.fn(),
  writeFileMock: jest.fn(),
};

export const fsRepositoryMock = {
  readFile: fsRepositoryMockFn.readFileMock,
  mkdir: fsRepositoryMockFn.mkdirMock,
  appendFile: fsRepositoryMockFn.appendFileMock,
  readdir: fsRepositoryMockFn.readdirMock,
  rename: fsRepositoryMockFn.renameMock,
  unlink: fsRepositoryMockFn.unlinkMock,
  writeFile: fsRepositoryMockFn.writeFileMock,
} as unknown as FsRepository;

export const osmdbtExecutableMockFn = {
  getLogMock: jest.fn(),
  createDiffMock: jest.fn(),
  catchupMock: jest.fn(),
};

export const osmdbtExecutableMock = {
  getLog: osmdbtExecutableMockFn.getLogMock,
  createDiff: osmdbtExecutableMockFn.createDiffMock,
  catchup: osmdbtExecutableMockFn.catchupMock,
} as unknown as OsmdbtExecutable;

export const osmiumExecutableMockFn = {
  fileInfoMock: jest.fn(),
};

export const osmiumExecutableMock = {
  fileInfo: osmiumExecutableMockFn.fileInfoMock,
} as unknown as OsmiumExecutable;

export const mediatorMockFn = {
  reserveAccessMock: jest.fn(),
  removeLockMock: jest.fn(),
  updateActionMock: jest.fn(),
  createActionMock: jest.fn(),
};

export const mediatorMock = {
  reserveAccess: mediatorMockFn.reserveAccessMock,
  removeLock: mediatorMockFn.removeLockMock,
  updateAction: mediatorMockFn.updateActionMock,
  createAction: mediatorMockFn.createActionMock,
} as unknown as StatefulMediator;
