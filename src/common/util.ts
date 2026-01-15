import { Readable } from 'stream';
import { contentType } from 'mime-types';
import { DIFF_TOP_DIR_DIVIDER, DIFF_BOTTOM_DIR_DIVIDER, DIFF_STATE_FILE_MODULO, SEQUENCE_NUMBER_COMPONENT_LENGTH } from '../common/constants';

interface Success<T> {
  data: T;
}

interface Failure<E> {
  error: E;
}

type Result<T, E = Error> = Success<T> | Failure<E>;

const normalizeChunk = (chunk: unknown): Buffer => {
  if (Buffer.isBuffer(chunk)) return chunk;
  if (typeof chunk === 'string') return Buffer.from(chunk, 'utf8');
  if (chunk instanceof Uint8Array) return Buffer.from(chunk);
  throw new TypeError('Unsupported stream chunk type');
};

export async function attemptSafely<T, E = Error>(fn: () => Promise<T>): Promise<Result<T, E>> {
  try {
    const data = await fn();
    return { data };
  } catch (error) {
    return { error: error as E };
  }
}

export const streamToString = async (stream?: Readable): Promise<string> => {
  if (stream === undefined) return '';

  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (chunk) => chunks.push(normalizeChunk(chunk)));
    stream.on('error', reject);
    stream.on('end', () => {
      stream.removeAllListeners();
      resolve(Buffer.concat(chunks).toString('utf8'));
    });
  });
};

export const getDiffDirPathComponents = (sequenceNumberString: string): [string, string, string] => {
  const sequenceNumberInt = parseInt(sequenceNumberString);

  const top = sequenceNumberInt / DIFF_TOP_DIR_DIVIDER;
  const bottom = (sequenceNumberInt % DIFF_TOP_DIR_DIVIDER) / DIFF_BOTTOM_DIR_DIVIDER;
  const state = sequenceNumberInt % DIFF_STATE_FILE_MODULO;

  return [top, bottom, state].map((component) => {
    const floored = Math.floor(component);
    return floored.toString().padStart(SEQUENCE_NUMBER_COMPONENT_LENGTH, '0');
  }) as [string, string, string];
};

export const evaluateContentType = (key: string): string | undefined => {
  let evaluatedContentType: string | undefined = undefined;

  const fetchedTypeFromKey = key.split('/').pop();

  if (fetchedTypeFromKey !== undefined) {
    const type = contentType(fetchedTypeFromKey);
    evaluatedContentType = type !== false ? type : undefined;
  }

  return evaluatedContentType;
};

export const extractSequenceNumber = (content: string): string => {
  const matchResult = content.match(/sequenceNumber=\d+/);
  if (matchResult === null || matchResult.length === 0) {
    throw new Error('failed to extract sequece number');
  }

  const sequenceNumber = matchResult[0].split('=')[1]!;

  return sequenceNumber;
};
export const delay = async (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

export const stubHealthCheck = async (): Promise<void> => Promise.resolve();
