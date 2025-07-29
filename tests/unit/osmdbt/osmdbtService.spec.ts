import * as fsPromises from 'fs/promises';
import { readFile } from 'fs/promises';
import { StatefulMediator } from '@map-colonies/arstotzka-mediator';
import jsLogger from '@map-colonies/js-logger';
import { trace } from '@opentelemetry/api';
import { getConfig, initConfig } from '@src/common/config';
import { ExitCodes, SERVICE_NAME } from '@src/common/constants';
import { tracingFactory } from '@src/common/tracing';
import { OsmdbtService } from '@src/osmdbt/osmdbtService';
import { S3Manager } from '@src/s3/s3Manager';

jest.mock('fs/promises');

let osmdbtService: OsmdbtService;
describe('OsmdbtService', () => {
  const getStateFileFromS3ToFs = jest.fn();
  const uploadFile = jest.fn();
  const reserveAccess = jest.fn();
  const removeLock = jest.fn();
  const updateAction = jest.fn();
  const createAction = jest.fn();

  beforeAll(async () => {
    await initConfig(true);

    const config = getConfig();

    const tracingConfig = config.get('telemetry.tracing');
    const sharedConfig = config.get('telemetry.shared');

    const tracing = tracingFactory({ ...tracingConfig, ...sharedConfig });

    tracing.start();
  });

  beforeEach(() => {
    const s3Manager = {
      getStateFileFromS3ToFs,
      uploadFile,
    } as unknown as S3Manager;

    const mediator = {
      reserveAccess,
      removeLock,
      updateAction,
      createAction,
    } as unknown as StatefulMediator;

    osmdbtService = new OsmdbtService(
      jsLogger({ enabled: false }),
      trace.getTracer(`${SERVICE_NAME}_osmdbtService_unit_test`),
      getConfig(),
      mediator,
      s3Manager
    );
  });

  describe('isJobActive', () => {
    it('should return false by default', () => {
      expect(OsmdbtService.isJobActive()).toBe(false);
    });
    it('should return true when a job is active', async () => {
      reserveAccess.mockResolvedValue(undefined);
      getStateFileFromS3ToFs.mockResolvedValue(undefined);
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
      jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'collectInfo' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService).mockImplementation(async () => {});
      await osmdbtService.startJob();
      expect(OsmdbtService.isJobActive()).toBe(true);
    });
  });

  describe('startJob', () => {
    beforeEach(() => {
      jest.clearAllMocks();
    });
    it('should not start a job if one is already active', async () => {
      OsmdbtService['isActiveJob'] = true;
      const loggerWarnSpy = jest.spyOn(osmdbtService['logger'], 'warn');
      await osmdbtService.startJob();
      expect(loggerWarnSpy).toHaveBeenCalledWith(expect.objectContaining({ msg: 'job is already active, skipping the start' }));
      OsmdbtService['isActiveJob'] = false;
    });
    it('should handle errors and call processExitSafely with error code', async () => {
      reserveAccess.mockRejectedValue(new Error('fail'));
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await osmdbtService.startJob();
      expect(processExitSafelySpy).toHaveBeenCalled();
    });
    it('should complete successfully and call mediator methods', async () => {
      reserveAccess.mockResolvedValue(undefined);
      getStateFileFromS3ToFs.mockResolvedValue(undefined);
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
      jest
        .spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService)
        .mockImplementationOnce(async () => {})
        .mockImplementationOnce(async () => {});
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'collectInfo' as keyof OsmdbtService).mockImplementation(async () => {});
      jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService).mockImplementation(async () => {});
      await osmdbtService.startJob();
      expect(reserveAccess).toHaveBeenCalled();
      expect(getStateFileFromS3ToFs).toHaveBeenCalled();
      expect(removeLock).toHaveBeenCalled();
    });
  });

  describe('getSequenceNumber', () => {
    it('should return the sequence number if present', async () => {
      (readFile as jest.MockedFunction<typeof readFile>).mockResolvedValue('sequenceNumber=123');
      const result = await (osmdbtService as unknown as { getSequenceNumber: () => Promise<string> }).getSequenceNumber();
      expect(result).toBe('123');
    });
    it('should throw if sequence number is missing', async () => {
      (readFile as jest.MockedFunction<typeof readFile>).mockResolvedValue('no sequence');
      await expect((osmdbtService as unknown as { getSequenceNumber: () => Promise<string> }).getSequenceNumber()).rejects.toThrow();
    });
    it('should throw if sequence number is not a string', async () => {
      (readFile as jest.MockedFunction<typeof readFile>).mockResolvedValue('sequenceNumber=');
      await expect((osmdbtService as unknown as { getSequenceNumber: () => Promise<string> }).getSequenceNumber()).rejects.toThrow();
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
      expect(OsmdbtService.isJobActive()).toBe(false);
      expect(endMock).toHaveBeenCalled();
    });
  });

  describe('uploadDiff', () => {
    it.skip('should upload files successfully', async () => {
      //TODO: work on test
      jest.spyOn(osmdbtService as unknown as { osmdbtConfig: { changesDir: string } }, 'osmdbtConfig', 'get').mockReturnValue({ changesDir: '' });
      jest.spyOn(osmdbtService as unknown as { config: { isEnabled: boolean } }, 'config', 'get').mockReturnValue({ isEnabled: false });
      jest.spyOn(fsPromises, 'readFile').mockResolvedValue(Buffer.from('data'));
      jest
        .spyOn(osmdbtService as unknown as { s3Manager: { uploadFile: jest.Mock } }, 's3Manager', 'get')
        .mockReturnValue({ uploadFile: jest.fn().mockResolvedValue(undefined) });
      await (osmdbtService as unknown as { uploadDiff: (sequenceNumber: string) => Promise<void> }).uploadDiff('123');
    });

    it('should handle error in file upload', async () => {
      (readFile as jest.MockedFunction<typeof readFile>).mockResolvedValue('sequenceNumber=123');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);

      (osmdbtService as unknown as { s3Manager: S3Manager }).s3Manager.uploadFile = jest.fn().mockRejectedValue('fail uploadFile');

      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);

      await osmdbtService.startJob();

      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.S3_ERROR);
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
    beforeEach(() => {
      jest.clearAllMocks();
    });

    it('should handle error in prepareEnvironment', async () => {
      reserveAccess.mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'prepareEnvironment' as keyof OsmdbtService).mockRejectedValue(new Error('fail prepareEnvironment'));
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);

      await osmdbtService.startJob();

      expect(processExitSafelySpy).toHaveBeenCalledWith(ExitCodes.GENERAL_ERROR);
    });

    it('should handle error in getStateFileFromS3ToFs', async () => {
      reserveAccess.mockResolvedValue(undefined);

      (osmdbtService as unknown as { s3Manager: S3Manager }).s3Manager.getStateFileFromS3ToFs = jest.fn().mockRejectedValue('getStateFileFromS3ToFs');

      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);

      await osmdbtService.startJob();

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
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService).mockRejectedValue(new Error('fail getSequenceNumber'));
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await osmdbtService.startJob();
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
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockRejectedValue(new Error('fail uploadDiff'));
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await osmdbtService.startJob();
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
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockRejectedValue({ error: new Error('fail commit') });
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await osmdbtService.startJob();
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
      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockRejectedValue({ error: new Error('fail commit') });
      jest.spyOn(osmdbtService, 'rollback' as keyof OsmdbtService).mockRejectedValue({ error: new Error('fail rollback') });
      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await osmdbtService.startJob();
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

      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'collectInfo' as keyof OsmdbtService).mockRejectedValue(new Error('fail collectInfo'));

      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await osmdbtService.startJob();
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

      jest.spyOn(tracer, 'startActiveSpan').mockImplementation((name, opts, ctx, fn) => fn(mockSpan));
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValueOnce('1');
      (jest.spyOn(osmdbtService, 'getSequenceNumber' as keyof OsmdbtService) as jest.Mock).mockResolvedValue('2');
      //@ts-expect-error
      jest.spyOn(osmdbtService, 'runCommand' as keyof OsmdbtService).mockResolvedValue('{"test": "test"}');
      jest.spyOn(osmdbtService, 'uploadDiff' as keyof OsmdbtService).mockResolvedValue(undefined);
      jest.spyOn(osmdbtService, 'commitChanges' as keyof OsmdbtService).mockResolvedValue(undefined);

      const processExitSafelySpy = jest.spyOn(osmdbtService, 'processExitSafely' as keyof OsmdbtService);
      await osmdbtService.startJob();
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
      await expect(osmdbtService.startJob()).rejects.toThrow('fail processExitSafely');
    });
  });
});
