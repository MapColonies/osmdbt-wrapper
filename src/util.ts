import { contentType } from 'mime-types';
import { DIFF_TOP_DIR_DIVIDER, DIFF_BOTTOM_DIR_DIVIDER, DIFF_STATE_FILE_MODULO, SEQUENCE_NUMBER_COMPONENT_LENGTH } from './common/constants';

export const streamToString = async (stream: NodeJS.ReadStream): Promise<string> => {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
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

export const delay = async (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
