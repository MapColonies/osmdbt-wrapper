import { Readable } from 'stream';
import { ActionStatus } from '@map-colonies/arstotzka-common';
import jsLogger from '@map-colonies/js-logger';
import { trace } from '@opentelemetry/api';
import { Registry } from 'prom-client';
import { ConfigType, getConfig, initConfig } from '@src/common/config';
import { BACKUP_DIR_NAME, ExitCodes, SERVICE_NAME, STATE_FILE } from '@src/common/constants';
import { OsmdbtService } from '@src/osmdbt/osmdbtService';
import { ErrorWithExitCode } from '@src/common/errors';
import {
  fsRepositoryMock,
  fsRepositoryMockFn,
  osmdbtExecutableMock,
  osmdbtExecutableMockFn,
  osmiumExecutableMockFn,
  osmiumExecutableMock,
  s3ManagerMock,
  s3ManagerMockFn,
  mediatorMock,
  mediatorMockFn,
} from '@tests/mocks';

const PREV_STATE_FILE_CONTENT = 'sequenceNumber=666';
const NEXT_STATE_FILE_CONTENT = 'sequenceNumber=667';

describe('OsmdbtService', () => {
  let osmdbtService: OsmdbtService;
  let config: ConfigType;

  beforeAll(async () => {
    await initConfig(true);

    config = getConfig();
    config.get('osmdbt');

    const logger = jsLogger({ enabled: false });

    osmdbtService = new OsmdbtService(
      logger,
      trace.getTracer(`${SERVICE_NAME}_osmdbtService_unit_test`),
      config,
      mediatorMock,
      s3ManagerMock,
      fsRepositoryMock,
      osmdbtExecutableMock,
      osmiumExecutableMock,
      new Registry()
    );
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('executeJob', () => {
    it('should not start a job if a job is already active', async () => {
      osmdbtService['isActiveJob'] = true;

      await expect(osmdbtService.executeJob()).resolves.not.toThrow();

      expect(mediatorMockFn.reserveAccessMock).not.toHaveBeenCalled();
      osmdbtService['isActiveJob'] = false;
    });

    it('should throw if could not reserve access', async () => {
      const error = new Error('access error');
      mediatorMockFn.reserveAccessMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if could not mkdir any environment', async () => {
      const error = new Error('fs error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.mkdirMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.changesDir);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.runDir);
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if could not mkdir the whole environment', async () => {
      const error = new Error('fs error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.mkdirMock.mockResolvedValueOnce(undefined).mockResolvedValueOnce(undefined).mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.changesDir);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.runDir);
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if could not pull state file from s3', async () => {
      const error = new Error('s3 error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if could not save pulled state file on both workdir and backup', async () => {
      const error = new Error('fs error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/${STATE_FILE}`, PREV_STATE_FILE_CONTENT);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledWith(
        `${config.get('osmdbt')?.changesDir}/${BACKUP_DIR_NAME}/${STATE_FILE}`,
        PREV_STATE_FILE_CONTENT
      );
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if could not save pulled state file on either workdir and backup', async () => {
      const error = new Error('fs error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined).mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/${STATE_FILE}`, PREV_STATE_FILE_CONTENT);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledWith(
        `${config.get('osmdbt')?.changesDir}/${BACKUP_DIR_NAME}/${STATE_FILE}`,
        PREV_STATE_FILE_CONTENT
      );
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if failed getting prev sequence number due to readFile error', async () => {
      const error = new Error('fs error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.readFileMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/${STATE_FILE}`, 'utf-8');
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if failed getting prev sequence number due to invalid content', async () => {
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce('invalid');

      await expect(osmdbtService.executeJob()).rejects.toThrow(
        new ErrorWithExitCode(`failed to fetch sequence number out of the state file, ${STATE_FILE} is invalid`, ExitCodes.INVALID_STATE_FILE_ERROR)
      );

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/${STATE_FILE}`, 'utf-8');
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if osmdbt:get-log failed', async () => {
      const error = new Error('osmdbt get-log error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if osmdbt:create-diff failed', async () => {
      const error = new Error('osmdbt create-diff error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if failed getting next sequence number due to readFile error', async () => {
      const error = new Error('fs error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(2);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenNthCalledWith(1, `${config.get('osmdbt')?.changesDir}/${STATE_FILE}`, 'utf-8');
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenNthCalledWith(2, `${config.get('osmdbt')?.changesDir}/${STATE_FILE}`, 'utf-8');
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if failed getting next sequence number due to invalid content error', async () => {
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce('invalid');

      await expect(osmdbtService.executeJob()).rejects.toThrow(
        new ErrorWithExitCode(`failed to fetch sequence number out of the state file, ${STATE_FILE} is invalid`, ExitCodes.INVALID_STATE_FILE_ERROR)
      );

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(2);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should resolve with no errors if prev state is equal to next state', async () => {
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      mediatorMockFn.removeLockMock.mockResolvedValueOnce(undefined);

      await expect(osmdbtService.executeJob()).resolves.not.toThrow();

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(2);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should resolve with no errors if prev state is equal to next state and remove lock throws', async () => {
      const error = new Error('remove lock error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      mediatorMockFn.removeLockMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).resolves.not.toThrow();

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(2);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if creating an action throws an error', async () => {
      const error = new Error('create action error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(2);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should not throw an error even if removing the lock after action creation throws', async () => {
      const error = new Error('remove lock error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT).mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      mediatorMockFn.removeLockMock.mockRejectedValueOnce(error);
      fsRepositoryMockFn.readdirMock.mockReturnValue([]);

      await expect(osmdbtService.executeJob()).resolves.not.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(5);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.COMPLETED, metadata: {} });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if upload diff throws due to readfile', async () => {
      const error = new Error('readfile error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockRejectedValueOnce(error);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(4);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if upload diff throws due to s3 put object', async () => {
      const error = new Error('put object error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValue(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.putObjectMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(4);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(2);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledWith('000/000/667.state.txt', NEXT_STATE_FILE_CONTENT);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledWith('000/000/667.osc.gz', NEXT_STATE_FILE_CONTENT);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if upload diff throws due to s3 put object even if update action fails', async () => {
      const error = new Error('put object error');
      const updateActionError = new Error('update action error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValue(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.putObjectMock.mockRejectedValueOnce(error);
      mediatorMockFn.updateActionMock.mockRejectedValueOnce(updateActionError);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(4);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(2);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledWith('000/000/667.state.txt', NEXT_STATE_FILE_CONTENT);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledWith('000/000/667.osc.gz', NEXT_STATE_FILE_CONTENT);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if upload diff throws due to state s3 put object', async () => {
      const error = new Error('put object error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValue(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.putObjectMock.mockResolvedValueOnce(undefined).mockResolvedValueOnce(undefined).mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(5);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledWith('000/000/667.state.txt', NEXT_STATE_FILE_CONTENT);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledWith('000/000/667.osc.gz', NEXT_STATE_FILE_CONTENT);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledWith('state.txt', NEXT_STATE_FILE_CONTENT);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should rollback and throw an error if marking logs throws due to readdir error', async () => {
      const error = new Error('readdir error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValue(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(6);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(4);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should rollback and throw an error if marking logs throws due to rename error', async () => {
      const error = new Error('rename error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValue(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValue(['/mock.done', '/mock.test']);
      fsRepositoryMockFn.renameMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(6);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(4);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(fsRepositoryMockFn.renameMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.renameMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.logDir}/mock.done`, `${config.get('osmdbt')?.logDir}/mock`);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should rollback and throw an error if osmdbt catchup throws', async () => {
      const error = new Error('catchup error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValue(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValue([]);
      osmdbtExecutableMockFn.catchupMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(6);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(4);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should rollback and throw an error if osmdbt catchup throws even if update action fails', async () => {
      const error = new Error('catchup error');
      const updateActionError = new Error('update action error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValue(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValue([]);
      osmdbtExecutableMockFn.catchupMock.mockRejectedValueOnce(error);
      mediatorMockFn.updateActionMock.mockRejectedValueOnce(updateActionError);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(6);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(4);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if rollback throws due to readfile error', async () => {
      const error = new Error('readfile error');
      const catchupError = new Error('catchup error');
      const expectedError = new ErrorWithExitCode('rollback error', ExitCodes.FS_ERROR);
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(PREV_STATE_FILE_CONTENT)
        .mockRejectedValueOnce(error);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValue([]);
      osmdbtExecutableMockFn.catchupMock.mockRejectedValueOnce(catchupError);

      await expect(osmdbtService.executeJob()).rejects.toStrictEqual(expectedError);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(6);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(3);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error: expectedError } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if rollback throws due to put object error', async () => {
      const error = new Error('put object error');
      const catchupError = new Error('catchup error');
      const expectedError = new ErrorWithExitCode('rollback error', ExitCodes.FS_ERROR);
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValue([]);
      osmdbtExecutableMockFn.catchupMock.mockRejectedValueOnce(catchupError);
      s3ManagerMockFn.putObjectMock
        .mockResolvedValueOnce(undefined)
        .mockResolvedValueOnce(undefined)
        .mockResolvedValueOnce(undefined)
        .mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toStrictEqual(expectedError);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(6);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(4);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledWith(STATE_FILE, PREV_STATE_FILE_CONTENT);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error: expectedError } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if rollback throws due to put object error even if update action fails', async () => {
      const error = new Error('put object error');
      const catchupError = new Error('catchup error');
      const expectedError = new ErrorWithExitCode('rollback error', ExitCodes.FS_ERROR);
      const updateActionError = new Error('update action error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT)
        .mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValue([]);
      osmdbtExecutableMockFn.catchupMock.mockRejectedValueOnce(catchupError);
      s3ManagerMockFn.putObjectMock
        .mockResolvedValueOnce(undefined)
        .mockResolvedValueOnce(undefined)
        .mockResolvedValueOnce(undefined)
        .mockRejectedValueOnce(error);
      mediatorMockFn.updateActionMock.mockRejectedValueOnce(updateActionError);

      await expect(osmdbtService.executeJob()).rejects.toStrictEqual(expectedError);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(6);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(4);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledWith(STATE_FILE, PREV_STATE_FILE_CONTENT);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error: expectedError } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if post catchup throws due to readdir error', async () => {
      const error = new Error('readdir error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValueOnce([]).mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(5);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.putObjectMock).not.toHaveBeenCalledWith(STATE_FILE, PREV_STATE_FILE_CONTENT);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if post catchup throws due to unlink error', async () => {
      const error = new Error('unlink error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce(['666.osc.gz', '666.state.txt'])
        .mockResolvedValueOnce(['mock.log']);
      fsRepositoryMockFn.unlinkMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(5);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.putObjectMock).not.toHaveBeenCalledWith(STATE_FILE, PREV_STATE_FILE_CONTENT);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(3);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/000/000`);
      expect(fsRepositoryMockFn.unlinkMock).toHaveBeenCalledTimes(3);
      expect(fsRepositoryMockFn.unlinkMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.logDir}/mock.log`);
      expect(fsRepositoryMockFn.unlinkMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/000/000/666.osc.gz`);
      expect(fsRepositoryMockFn.unlinkMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/000/000/666.state.txt`);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if post catchup throws even if update action fails', async () => {
      const error = new Error('unlink error');
      const updateActionError = new Error('update action error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValueOnce([]).mockResolvedValueOnce(['666.osc.gz']).mockResolvedValueOnce(['mock.log']);
      fsRepositoryMockFn.unlinkMock.mockRejectedValueOnce(error);
      mediatorMockFn.updateActionMock.mockRejectedValueOnce(updateActionError);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(5);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.putObjectMock).not.toHaveBeenCalledWith(STATE_FILE, PREV_STATE_FILE_CONTENT);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(3);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/000/000`);
      expect(fsRepositoryMockFn.unlinkMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.unlinkMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.logDir}/mock.log`);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.FAILED, metadata: { error } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should resolve without errors even if osmium fileInfo fails', async () => {
      const error = new Error('fileInfo error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValueOnce([]).mockResolvedValue([]).mockResolvedValue([]);
      osmiumExecutableMockFn.fileInfoMock.mockRejectedValueOnce(error);

      await expect(osmdbtService.executeJob()).resolves.not.toThrow();

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(5);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.putObjectMock).not.toHaveBeenCalledWith(STATE_FILE, PREV_STATE_FILE_CONTENT);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(3);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/000/000`);
      expect(osmiumExecutableMockFn.fileInfoMock).toHaveBeenCalledTimes(1);
      expect(osmiumExecutableMockFn.fileInfoMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/000/000/667.osc.gz`);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.COMPLETED, metadata: {} });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should resolve without errors and attribute action with fileInfo output', async () => {
      const expectedMetadata = { mock: true, key: 'value' };
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValue([]);
      osmiumExecutableMockFn.fileInfoMock.mockResolvedValueOnce(JSON.stringify(expectedMetadata));

      await expect(osmdbtService.executeJob()).resolves.not.toThrow();

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(5);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.putObjectMock).not.toHaveBeenCalledWith(STATE_FILE, PREV_STATE_FILE_CONTENT);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(3);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/000/000`);
      expect(osmiumExecutableMockFn.fileInfoMock).toHaveBeenCalledTimes(1);
      expect(osmiumExecutableMockFn.fileInfoMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/000/000/667.osc.gz`);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.COMPLETED, metadata: { info: expectedMetadata } });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });

    it('should throw if after successful job update action throws', async () => {
      const updateActionError = new Error('update action error');
      mediatorMockFn.reserveAccessMock.mockResolvedValueOnce(undefined);
      s3ManagerMockFn.getObjectMock.mockResolvedValueOnce(Readable.from([PREV_STATE_FILE_CONTENT]));
      fsRepositoryMockFn.writeFileMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(PREV_STATE_FILE_CONTENT);
      osmdbtExecutableMockFn.getLogMock.mockResolvedValueOnce(undefined);
      osmdbtExecutableMockFn.createDiffMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readFileMock.mockResolvedValueOnce(NEXT_STATE_FILE_CONTENT);
      mediatorMockFn.createActionMock.mockResolvedValueOnce(undefined);
      fsRepositoryMockFn.readdirMock.mockResolvedValueOnce([]);
      mediatorMockFn.updateActionMock.mockRejectedValueOnce(updateActionError);

      await expect(osmdbtService.executeJob()).rejects.toThrow(updateActionError);

      expect(mediatorMockFn.reserveAccessMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.mkdirMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.getObjectMock).toHaveBeenCalledWith(STATE_FILE);
      expect(fsRepositoryMockFn.writeFileMock).toHaveBeenCalledTimes(2);
      expect(fsRepositoryMockFn.readFileMock).toHaveBeenCalledTimes(5);
      expect(osmdbtExecutableMockFn.getLogMock).toHaveBeenCalledTimes(1);
      expect(osmdbtExecutableMockFn.createDiffMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.createActionMock).toHaveBeenCalledWith({ state: 667 });
      expect(mediatorMockFn.removeLockMock).toHaveBeenCalledTimes(1);
      expect(s3ManagerMockFn.putObjectMock).toHaveBeenCalledTimes(3);
      expect(s3ManagerMockFn.putObjectMock).not.toHaveBeenCalledWith(STATE_FILE, PREV_STATE_FILE_CONTENT);
      expect(osmdbtExecutableMockFn.catchupMock).toHaveBeenCalledTimes(1);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledTimes(3);
      expect(fsRepositoryMockFn.readdirMock).toHaveBeenCalledWith(config.get('osmdbt')?.logDir);
      expect(osmiumExecutableMockFn.fileInfoMock).toHaveBeenCalledTimes(1);
      expect(osmiumExecutableMockFn.fileInfoMock).toHaveBeenCalledWith(`${config.get('osmdbt')?.changesDir}/000/000/667.osc.gz`);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledTimes(1);
      expect(mediatorMockFn.updateActionMock).toHaveBeenCalledWith({ status: ActionStatus.COMPLETED, metadata: {} });
      expect(osmdbtService['isActiveJob']).toBeFalsy();
    });
  });
});
