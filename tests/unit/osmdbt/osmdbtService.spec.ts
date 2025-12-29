import * as fsPromises from 'fs/promises';
import { Readable } from 'stream';
import execa from 'execa';
import { StatefulMediator } from '@map-colonies/arstotzka-mediator';
import jsLogger from '@map-colonies/js-logger';
import { trace } from '@opentelemetry/api';
import { getConfig, initConfig } from '@src/common/config';
import { ExitCodes, SERVICE_NAME } from '@src/common/constants';
import { tracingFactory } from '@src/common/tracing';
import { OsmdbtService } from '@src/osmdbt/osmdbtService';
import { S3Manager } from '@src/s3/s3Manager';
import { ErrorWithExitCode } from '@src/common/errors';
import { FsRepository } from '@src/fs/fsRepository';
import { Executable } from '@src/executables';
import { OsmiumExecutable } from '@src/executables/osmium';
import { OsmdbtExecutable } from '@src/executables/osmdbt';

jest.mock('fs/promises');
jest.mock('execa');

describe('OsmdbtService', () => {
  const getFile = jest.fn();
  const uploadFile = jest.fn();
  const reserveAccess = jest.fn();
  const removeLock = jest.fn();
  const updateAction = jest.fn();
  const createAction = jest.fn();

  const readFile = jest.fn();
  const mkdir = jest.fn();
  const appendFile = jest.fn();
  const readdir = jest.fn();
  const rename = jest.fn();
  const unlink = jest.fn();

  let osmdbtService: OsmdbtService;

  const s3Manager = {
    getFile,
    uploadFile,
  } as unknown as S3Manager;

  const fsRepository = {
    readFile,
    mkdir,
    appendFile,
    readdir,
    rename,
    unlink,
  } as unknown as FsRepository;

  const mediator = {
    reserveAccess,
    removeLock,
    updateAction,
    createAction,
  } as unknown as StatefulMediator;

  beforeAll(async () => {
    await initConfig(true);

    const config = getConfig();

    const tracingConfig = config.get('telemetry.tracing');
    const sharedConfig = config.get('telemetry.shared');

    const tracing = tracingFactory({ ...tracingConfig, ...sharedConfig });

    tracing.start();
  });

  beforeEach(() => {
    jest.clearAllMocks();

    const logger = jsLogger({ enabled: false });
    const config = getConfig();

    osmdbtService = new OsmdbtService(
      logger,
      trace.getTracer(`${SERVICE_NAME}_osmdbtService_unit_test`),
      config,
      mediator,
      s3Manager,
      fsRepository,
      new OsmdbtExecutable(logger, config),
      new OsmiumExecutable(logger, config)
    );
  });

  describe('isJobActive', () => {
    // it('should return false by default', () => {
    //   expect(OsmdbtService.isJobActive()).toBe(false);
    // });

    it.skip('should return true when a job is active', async () => {
      reserveAccess.mockResolvedValue(undefined);
      getFile.mockResolvedValue(undefined);
      uploadFile.mockResolvedValue(undefined);
      updateAction.mockResolvedValue(undefined);
      createAction.mockResolvedValue(undefined);
      removeLock.mockResolvedValue(undefined);
      const tracer = osmdbtService['tracer'];
      const mockSpan = {
        setAttribute: jest.fn(),
        setAttributes: jest.fn(),
        spanContext: jest.fn(),
        addEvent: jest.fn(),
        addLink: jest.fn(),
        addLinks: jest.fn(),
        end: jest.fn(),
        isRecording: jest.fn(),
        recordException: jest.fn(),
        setStatus: jest.fn(),
        updateName: jest.fn(),
      };
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      //@ts-expect-error spyOn a private function. it thinks it should only resolve as Promise<void>
      jest.spyOn(osmdbtService, 'pullStateFile' as keyof OsmdbtService).mockResolvedValueOnce(ExitCodes.SUCCESS);
      jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'collectInfo' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService).mockImplementation(async () => {});
      await expect(osmdbtService.executeJob()).resolves.not.toThrow();
      // expect(OsmdbtService.isJobActive()).toBe(true);
    });
  });

  describe('startJob', () => {
    // it('should not start a job if one is already active', async () => {
    //   OsmdbtService['isActiveJob'] = true;
    //   await expect(osmdbtService.startJob()).resolves.not.toThrow();
    //   expect(reserveAccess).not.toHaveBeenCalled();
    //   OsmdbtService['isActiveJob'] = false;
    // });

    it('should handle errors and call processExitSafely with error code', async () => {
      reserveAccess.mockRejectedValue(new Error('fail'));
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      const prepareEnvironmentSpy = jest.spyOn(osmdbtService, 'prepareEnvironment' as keyof OsmdbtService);

      await expect(osmdbtService.executeJob()).rejects.toThrow();

      expect(processExitSafelySpy).toHaveBeenCalledTimes(1);
      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.GENERAL_ERROR);
      expect(prepareEnvironmentSpy).not.toHaveBeenCalled();
    });

    it.skip('should complete successfully and call mediator methods', async () => {
      reserveAccess.mockResolvedValue(undefined);
      getFile.mockResolvedValue(undefined);
      uploadFile.mockResolvedValue(undefined);
      updateAction.mockResolvedValue(undefined);
      createAction.mockResolvedValue(undefined);
      const tracer = osmdbtService['tracer'];
      const mockSpan = {
        setAttribute: jest.fn(),
        setAttributes: jest.fn(),
        spanContext: jest.fn(),
        addEvent: jest.fn(),
        addLink: jest.fn(),
        addLinks: jest.fn(),
        end: jest.fn(),
        isRecording: jest.fn(),
        recordException: jest.fn(),
        setStatus: jest.fn(),
        updateName: jest.fn(),
      };
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      //@ts-expect-error spyOn a private function. it thinks it should only resolve as Promise<void>
      jest.spyOn(osmdbtService, 'pullStateFile' as keyof OsmdbtService).mockResolvedValueOnce(ExitCodes.SUCCESS);
      jest
        .spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService)
        .mockImplementationOnce(async () => {})
        .mockImplementationOnce(async () => {});
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'collectInfo' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService).mockImplementation(async () => {});
      await expect(osmdbtService.executeJob()).resolves.not.toThrow();
      expect(reserveAccess).toHaveBeenCalled();
      // expect(getFile).toHaveBeenCalled();
    });
  });

  describe('getSequenceNumber', () => {
    it('should return the sequence number if present', async () => {
      readFile.mockResolvedValue('sequenceNumber=123');
      const result = await osmdbtService['getSequenceNumber']();
      expect(result).toBe('123');
    });

    it('should throw if sequence number is missing', async () => {
      readFile.mockResolvedValue('no sequence');
      await expect(osmdbtService['getSequenceNumber']()).rejects.toThrow(
        new ErrorWithExitCode('failed to fetch sequence number out of the state file, state.txt is invalid', ExitCodes.INVALID_STATE_FILE_ERROR)
      );
    });

    it('should throw if sequence number is not a string', async () => {
      readFile.mockResolvedValue('sequenceNumber=');
      await expect(osmdbtService['getSequenceNumber']()).rejects.toThrow(
        new ErrorWithExitCode('failed to fetch sequence number out of the state file, state.txt is invalid', ExitCodes.INVALID_STATE_FILE_ERROR)
      );
    });
  });

  describe('processExitSafely', () => {
    it('should set isActiveJob to false and end span', () => {
      const endMock = jest.fn();
      interface MockRootJobSpan {
        setAttributes: jest.Mock;
        setStatus: jest.Mock;
        end: jest.Mock;
      }
      (osmdbtService as unknown as { rootJobSpan: MockRootJobSpan }).rootJobSpan = {
        setAttributes: jest.fn(),
        setStatus: jest.fn(),
        end: endMock,
      };
      (OsmdbtService as unknown as { isActiveJob: boolean }).isActiveJob = true;
      (osmdbtService as unknown as { processExitSafely: (code: number) => void }).processExitSafely(0);
      // expect(OsmdbtService.isJobActive()).toBe(false);
      expect(endMock).toHaveBeenCalled();
    });
  });

  describe('uploadDiff', () => {
    interface UploadDiff {
      uploadDiff: (sequenceNumber: string) => Promise<string>;
    }

    it('should run uploadDiff successfully', async () => {
      const serviceWithConfig = osmdbtService as unknown as { config: { get: (key: string) => unknown } };
      const originalGet = serviceWithConfig.config.get.bind(serviceWithConfig.config);

      jest.spyOn(serviceWithConfig.config, 'get').mockImplementation((key: string) => {
        if (key === 'telemetry.tracing') {
          return { isEnabled: true };
        }
        return originalGet(key);
      });

      await expect((osmdbtService as unknown as UploadDiff).uploadDiff('123')).resolves.toBeUndefined();
    });

    it('should handle error in file upload', async () => {
      //@ts-expect-error spyOn a private function. it thinks it should only resolve as Promise<void>
      jest.spyOn(osmdbtService, 'pullStateFile' as keyof OsmdbtService).mockResolvedValueOnce(ExitCodes.SUCCESS);
      (readFile as jest.MockedFunction<typeof readFile>).mockResolvedValue('sequenceNumber=123');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1').mockResolvedValueOnce('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);

      (osmdbtService as unknown as { s3Manager: S3Manager }).s3Manager.uploadFile = jest
        .fn()
        .mockRejectedValue(new ErrorWithExitCode('s3 error', ExitCodes.S3_ERROR));

      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);

      await expect(osmdbtService.executeJob()).rejects.toThrow();

      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.S3_ERROR);
    });
  });

  describe('pullStateFile', () => {
    interface PullStateFile {
      pullStateFile: () => Promise<(typeof ExitCodes)[keyof typeof ExitCodes]>;
    }

    const createStateFileReadStream = () => Readable.from([Buffer.from('sequenceNumber=123', 'utf-8')]) as NodeJS.ReadStream;

    it('should run pullStateFile successfully', async () => {
      (osmdbtService as unknown as { s3Manager: S3Manager }).s3Manager.getFile = jest.fn().mockResolvedValueOnce(createStateFileReadStream());

      (osmdbtService as unknown as { fsRepository: FsRepository }).fsRepository.writeFile = jest.fn().mockRejectedValue(undefined);

      await expect((osmdbtService as unknown as PullStateFile).pullStateFile()).resolves.toBe(ExitCodes.SUCCESS);

      (osmdbtService as unknown as { fsRepository: FsRepository }).fsRepository.writeFile = jest.fn().mockReset();
    });

    it('should handle error in file download', async () => {
      (osmdbtService as unknown as { s3Manager: S3Manager }).s3Manager.getFile = jest.fn().mockRejectedValueOnce('fail getFile');

      await expect((osmdbtService as unknown as PullStateFile).pullStateFile()).resolves.toBe(ExitCodes.S3_ERROR);
    });

    it('should handle error in file FS save', async () => {
      (osmdbtService as unknown as { s3Manager: S3Manager }).s3Manager.getFile = jest.fn().mockResolvedValueOnce(createStateFileReadStream());

      (osmdbtService as unknown as { fsRepository: FsRepository }).fsRepository.writeFile = jest.fn().mockRejectedValueOnce('fail gwriteFile');

      await expect((osmdbtService as unknown as PullStateFile).pullStateFile()).resolves.toBe(ExitCodes.FS_ERROR);
    });
  });

  describe('commitChanges', () => {
    it('should handle error in markLogFilesForCatchup', async () => {
      jest
        .spyOn(osmdbtService as unknown as { markLogFilesForCatchup: () => Promise<void> }, 'markLogFilesForCatchup')
        .mockRejectedValue(new Error('fail'));

      await expect((osmdbtService as unknown as { commitChanges: () => Promise<void> }).commitChanges()).rejects.toThrow('fail');
    });
    it('should handle error in runCommand', async () => {
      jest.spyOn(osmdbtService as unknown as { markLogFilesForCatchup: () => Promise<void> }, 'markLogFilesForCatchup').mockResolvedValue(undefined);
      jest.spyOn(osmdbtService as unknown as { runCommand: () => Promise<void> }, 'runCommand').mockRejectedValue(new Error('fail'));
      await expect((osmdbtService as unknown as { commitChanges: () => Promise<void> }).commitChanges()).rejects.toThrow('fail');
    });
    it('should handle error in postCatchupCleanup', async () => {
      jest.spyOn(osmdbtService as unknown as { markLogFilesForCatchup: () => Promise<void> }, 'markLogFilesForCatchup').mockResolvedValue(undefined);
      jest.spyOn(osmdbtService as unknown as { runCommand: () => Promise<void> }, 'runCommand').mockResolvedValue(undefined);
      jest.spyOn(osmdbtService as unknown as { postCatchupCleanup: () => Promise<void> }, 'postCatchupCleanup').mockRejectedValue(new Error('fail'));
      await expect((osmdbtService as unknown as { commitChanges: () => Promise<void> }).commitChanges()).rejects.toThrow('fail');
    });
  });

  describe('startJob edge cases', () => {
    it('should handle error in prepareEnvironment', async () => {
      const error = new Error('fail prepareEnvironment');
      reserveAccess.mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'prepareEnvironment' as keyof OsmdbtService).mockRejectedValue(error);
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.GENERAL_ERROR);
    });

    it('should handle error in s3 getFile', async () => {
      const error = new ErrorWithExitCode('s3 error', ExitCodes.S3_ERROR);
      reserveAccess.mockResolvedValue(undefined);
      s3Manager.getFile = jest.fn().mockRejectedValue(error);

      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);

      await expect(osmdbtService.executeJob()).rejects.toThrow(error);

      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.S3_ERROR);
    });

    it('should handle error in runCommand', async () => {
      reserveAccess.mockResolvedValue(undefined);
      const tracer = osmdbtService['tracer'];
      const mockSpan = {
        setAttribute: jest.fn(),
        setAttributes: jest.fn(),
        spanContext: jest.fn(),
        addEvent: jest.fn(),
        addLink: jest.fn(),
        addLinks: jest.fn(),
        end: jest.fn(),
        isRecording: jest.fn(),
        recordException: jest.fn(),
        setStatus: jest.fn(),
        updateName: jest.fn(),
      };
      //@ts-expect-error spyOn a private function. it thinks it should only resolve as Promise<void>
      jest.spyOn(osmdbtService, 'pullStateFile' as keyof OsmdbtService).mockResolvedValueOnce(ExitCodes.SUCCESS);
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService).mockRejectedValue(new Error('fail getSequenceNumber'));
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await expect(osmdbtService.executeJob()).resolves.not.toThrow();
      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.GENERAL_ERROR);
    });

    it('should handle error in uploadDiff', async () => {
      reserveAccess.mockResolvedValue(undefined);
      const tracer = osmdbtService['tracer'];
      const mockSpan = {
        setAttribute: jest.fn(),
        setAttributes: jest.fn(),
        spanContext: jest.fn(),
        addEvent: jest.fn(),
        addLink: jest.fn(),
        addLinks: jest.fn(),
        end: jest.fn(),
        isRecording: jest.fn(),
        recordException: jest.fn(),
        setStatus: jest.fn(),
        updateName: jest.fn(),
      };

      //@ts-expect-error spyOn a private function. it thinks it should only resolve as Promise<void>
      jest.spyOn(osmdbtService, 'pullStateFile' as keyof OsmdbtService).mockResolvedValueOnce(ExitCodes.SUCCESS);
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockRejectedValue(new Error('fail uploadDiff'));
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await expect(osmdbtService.executeJob()).resolves.not.toThrow();
      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.S3_ERROR);
    });

    it('should handle error in commitChanges failure', async () => {
      (readFile as jest.MockedFunction<typeof readFile>).mockResolvedValueOnce(Buffer.from('sequenceNumber=2'));

      reserveAccess.mockResolvedValue(undefined);
      const tracer = osmdbtService['tracer'];
      const mockSpan = {
        setAttribute: jest.fn(),
        setAttributes: jest.fn(),
        spanContext: jest.fn(),
        addEvent: jest.fn(),
        addLink: jest.fn(),
        addLinks: jest.fn(),
        end: jest.fn(),
        isRecording: jest.fn(),
        recordException: jest.fn(),
        setStatus: jest.fn(),
        updateName: jest.fn(),
      };
      //@ts-expect-error spyOn a private function. it thinks it should only resolve as Promise<void>
      jest.spyOn(osmdbtService, 'pullStateFile' as keyof OsmdbtService).mockResolvedValueOnce(ExitCodes.SUCCESS);
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockRejectedValue({ error: new Error('fail commit') });
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await expect(osmdbtService.executeJob()).resolves.not.toThrow();
      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.GENERAL_ERROR);
    });

    it('should handle error in commitChanges and rollback failure', async () => {
      (readFile as jest.MockedFunction<typeof readFile>).mockResolvedValueOnce(Buffer.from('sequenceNumber=2'));

      reserveAccess.mockResolvedValue(undefined);
      const tracer = osmdbtService['tracer'];
      const mockSpan = {
        setAttribute: jest.fn(),
        setAttributes: jest.fn(),
        spanContext: jest.fn(),
        addEvent: jest.fn(),
        addLink: jest.fn(),
        addLinks: jest.fn(),
        end: jest.fn(),
        isRecording: jest.fn(),
        recordException: jest.fn(),
        setStatus: jest.fn(),
        updateName: jest.fn(),
      };

      //@ts-expect-error spyOn a private function. it thinks it should only resolve as Promise<void>
      jest.spyOn(osmdbtService, 'pullStateFile' as keyof OsmdbtService).mockResolvedValueOnce(ExitCodes.SUCCESS);
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockRejectedValue({ error: new Error('fail commit') });
      jest.spyOn(osmdbtService, 'rollback' as keyof OsmdbtService).mockRejectedValue({ error: new Error('fail rollback') });
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await expect(osmdbtService.executeJob()).resolves.not.toThrow();
      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.ROLLBACK_FAILURE_ERROR);
    });

    it('should handle error in collectInfo', async () => {
      reserveAccess.mockResolvedValue(undefined);
      const tracer = osmdbtService['tracer'];
      const mockSpan = {
        setAttribute: jest.fn(),
        setAttributes: jest.fn(),
        spanContext: jest.fn(),
        addEvent: jest.fn(),
        addLink: jest.fn(),
        addLinks: jest.fn(),
        end: jest.fn(),
        isRecording: jest.fn(),
        recordException: jest.fn(),
        setStatus: jest.fn(),
        updateName: jest.fn(),
      };

      (osmdbtService as unknown as { appConfig: { [x: string]: unknown; shouldCollectInfo: boolean } }).appConfig = {
        ...osmdbtService['appConfig'],
        shouldCollectInfo: true,
      };
      //@ts-expect-error spyOn a private function. it thinks it should only resolve as Promise<void>
      jest.spyOn(osmdbtService, 'pullStateFile' as keyof OsmdbtService).mockResolvedValueOnce(ExitCodes.SUCCESS);
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'collectInfo' as keyof OsmdbtService).mockRejectedValue(new Error('fail collectInfo'));

      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await expect(osmdbtService.executeJob()).resolves.not.toThrow();
      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.GENERAL_ERROR);
    });

    it('should collectInfo true', async () => {
      reserveAccess.mockResolvedValue(undefined);
      const tracer = osmdbtService['tracer'];
      const mockSpan = {
        setAttribute: jest.fn(),
        setAttributes: jest.fn(),
        spanContext: jest.fn(),
        addEvent: jest.fn(),
        addLink: jest.fn(),
        addLinks: jest.fn(),
        end: jest.fn(),
        isRecording: jest.fn(),
        recordException: jest.fn(),
        setStatus: jest.fn(),
        updateName: jest.fn(),
      };

      (osmdbtService as unknown as { appConfig: { [x: string]: unknown; shouldCollectInfo: boolean } }).appConfig = {
        ...osmdbtService['appConfig'],
        shouldCollectInfo: true,
      };

      //@ts-expect-error spyOn a private function. it thinks it should only resolve as Promise<void>
      jest.spyOn(osmdbtService, 'pullStateFile' as keyof OsmdbtService).mockResolvedValueOnce(ExitCodes.SUCCESS);
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      //@ts-expect-error due to private function
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue('{"test": "test"}');
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockResolvedValue(undefined);

      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await expect(osmdbtService.executeJob()).resolves.not.toThrow();
      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.SUCCESS);
    });

    it('should handle error in processExitSafely', async () => {
      reserveAccess.mockResolvedValue(undefined);
      const tracer = osmdbtService['tracer'];
      const mockSpan = {
        setAttribute: jest.fn(),
        setAttributes: jest.fn(),
        spanContext: jest.fn(),
        addEvent: jest.fn(),
        addLink: jest.fn(),
        addLinks: jest.fn(),
        end: jest.fn(),
        isRecording: jest.fn(),
        recordException: jest.fn(),
        setStatus: jest.fn(),
        updateName: jest.fn(),
      };
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('1');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'collectInfo' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService).mockImplementation(() => {
        throw new Error('fail processExitSafely');
      });
      await expect(osmdbtService.executeJob()).rejects.toThrow('fail processExitSafely');
    });
  });

  describe('runCommand', () => {
    const mockedExeca = execa as jest.MockedFunction<typeof execa>;

    test.each<{
      executable: Executable;
      command: string;
      commandArgs: string[];
      resolvedExeca: execa.ExecaReturnValue<Buffer>;
    }>([
      {
        executable: 'osmdbt',
        command: 'GET_LOG',
        commandArgs: [],
        resolvedExeca: {
          command: 'osmdbt GET_LOG',
          escapedCommand: 'osmdbt GET_LOG',
          exitCode: 0,
          failed: false,
          killed: false,
          timedOut: false,
          isCanceled: false,
          signal: undefined,
          stdout: Buffer.from('mocked buffer'),
          stderr: Buffer.from(''),
          all: undefined,
        },
      },
    ])('should osmdbt runCommand with exit code 0', async ({ executable, command, commandArgs, resolvedExeca }) => {
      reserveAccess.mockResolvedValue(undefined);

      mockedExeca.mockResolvedValue(resolvedExeca);

      const result = await (
        osmdbtService as unknown as { runCommand: (executable: string, command: string, commandArgs: string[]) => Promise<string> }
      ).runCommand(executable, command, commandArgs);
      expect(result.toString()).toBe(resolvedExeca.stdout.toString());
    });

    test.each<{
      executable: Executable;
      command: string;
      commandArgs?: string[];
      resolvedExeca: execa.ExecaReturnValue<Buffer>;
    }>([
      {
        executable: 'osmdbt',
        command: 'GET_LOG',
        commandArgs: [],
        resolvedExeca: {
          command: 'osmdbt GET_LOG',
          escapedCommand: 'osmdbt GET_LOG',
          exitCode: 1,
          failed: false,
          killed: false,
          timedOut: false,
          isCanceled: false,
          signal: undefined,
          stdout: Buffer.from('mocked buffer'),
          stderr: Buffer.from('mocked error'),
          all: undefined,
        },
      },
      {
        executable: 'osmium',
        command: 'GET_LOG',
        resolvedExeca: {
          command: 'osmium GET_LOG',
          escapedCommand: 'osmium GET_LOG',
          exitCode: 1,
          failed: false,
          killed: false,
          timedOut: false,
          isCanceled: false,
          signal: undefined,
          stdout: Buffer.from(''),
          stderr: Buffer.from(''),
          all: undefined,
        },
      },
    ])('should fail runCommand', async ({ executable, command, commandArgs, resolvedExeca }) => {
      reserveAccess.mockResolvedValue(undefined);

      mockedExeca.mockResolvedValue(resolvedExeca);

      await expect(
        (osmdbtService as unknown as { runCommand: (executable: string, command: string, commandArgs?: string[]) => Promise<string> }).runCommand(
          executable,
          command,
          commandArgs
        )
      ).rejects.toThrow(resolvedExeca.stderr.toString());
    });

    it('should throw "executable errored" when error is not instance of Error', async () => {
      mockedExeca.mockRejectedValueOnce('non-error rejection');

      await expect(
        (osmdbtService as unknown as { runCommand: (executable: string, command: string, commandArgs?: string[]) => Promise<string> }).runCommand(
          'osmdbt',
          'TEST'
        )
      ).rejects.toThrow(new ErrorWithExitCode('osmdbt errored', ExitCodes.OSMDBT_ERROR));
    });
  });

  describe('rollback', () => {
    interface Rollback {
      rollback: () => Promise<string>;
    }
    it('should rollback successfully', async () => {
      uploadFile.mockResolvedValueOnce(true);

      await expect((osmdbtService as unknown as Rollback).rollback()).resolves.toBeUndefined();
    });

    it('should fail rollback', async () => {
      const error = new Error('S3 upload file mock error');
      uploadFile.mockRejectedValueOnce(error);

      await expect((osmdbtService as unknown as Rollback).rollback()).rejects.toThrow(error);
    });
  });

  describe('markLogFilesForCatchup', () => {
    interface MarkLogFilesForCatchup {
      markLogFilesForCatchup: () => Promise<string>;
    }

    it('should markLogFilesForCatchup successfully', async () => {
      readdir.mockResolvedValueOnce(['test', 'test.done'] as unknown as Awaited<ReturnType<typeof fsPromises.readdir>>);

      await expect((osmdbtService as unknown as MarkLogFilesForCatchup).markLogFilesForCatchup()).resolves.toBeUndefined();
    });

    it('should markLogFilesForCatchup fail because readdir fails', async () => {
      const error = new Error('some error');
      readdir.mockRejectedValueOnce(error);

      await expect((osmdbtService as unknown as MarkLogFilesForCatchup).markLogFilesForCatchup()).rejects.toBe(error);
    });

    it('should markLogFilesForCatchup fail because rename fails', async () => {
      readdir.mockResolvedValueOnce(['test', 'test.done'] as unknown as Awaited<ReturnType<typeof fsPromises.readdir>>);

      const error = new Error('some error');
      rename.mockRejectedValueOnce(error);

      await expect((osmdbtService as unknown as MarkLogFilesForCatchup).markLogFilesForCatchup()).rejects.toBe(error);
    });
  });

  describe('postCatchupCleanup', () => {
    interface PostCatchupCleanup {
      postCatchupCleanup: () => Promise<string>;
    }

    it('should postCatchupCleanup successfully', async () => {
      readdir.mockResolvedValueOnce(['test'] as unknown as Awaited<ReturnType<typeof fsPromises.readdir>>);
      unlink.mockResolvedValueOnce(undefined);

      await expect((osmdbtService as unknown as PostCatchupCleanup).postCatchupCleanup()).resolves.toBeUndefined();
    });

    it('should postCatchupCleanup throw error', async () => {
      const error = new Error('some error');
      readdir.mockRejectedValueOnce(error);

      await expect((osmdbtService as unknown as PostCatchupCleanup).postCatchupCleanup()).rejects.toBe(error);
    });
  });

  describe('prepareEnvironment', () => {
    interface PrepareEnvironment {
      prepareEnvironment: () => Promise<string>;
    }

    it('should prepareEnvironment successfully', async () => {
      mkdir.mockResolvedValueOnce(undefined);

      await expect((osmdbtService as unknown as PrepareEnvironment).prepareEnvironment()).resolves.toBeUndefined();
    });

    it('should prepareEnvironment because mkdir throws error', async () => {
      const error = new Error('some error');
      mkdir.mockRejectedValueOnce(error);

      await expect((osmdbtService as unknown as PrepareEnvironment).prepareEnvironment()).rejects.toBe(error);
    });
  });
});
