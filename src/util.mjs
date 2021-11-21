import { DIFF_TOP_DIR_DIVIDER, DIFF_BOTTOM_DIR_DIVIDER, DIFF_STATE_FILE_MODULO } from './constants.mjs';

export const streamToString = (stream) => {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
  });
};

export const getDiffDirPathComponents = (sequenceNumber) => {
  const top = sequenceNumber / DIFF_TOP_DIR_DIVIDER;
  const bottom = (sequenceNumber % DIFF_TOP_DIR_DIVIDER) / DIFF_BOTTOM_DIR_DIVIDER;
  const state = sequenceNumber % DIFF_STATE_FILE_MODULO;
  return [top, bottom, state].map((component) => {
    const intComponent = parseInt(component);
    return intComponent.toString().padStart(3, '0');
  });
};
