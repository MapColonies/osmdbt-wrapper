import { trace as traceAPI, Attributes, Context, Span, SpanStatusCode } from '@opentelemetry/api';
import { ErrorWithExitCode } from '../../errors';

export const TRACER_NAME = 'osmdbt-wrapper';

export const promisifySpan = async <T>(spanName: string, spanAttributes: Attributes, context: Context, fn: () => Promise<T>): Promise<T> => {
  // eslint-disable-next-line @typescript-eslint/no-misused-promises, no-async-promise-executor
  return new Promise(async (resolve, reject) => {
    const tracer = traceAPI.getTracer(TRACER_NAME);
    const span = tracer.startSpan(spanName, { attributes: spanAttributes }, context);

    try {
      const result = await fn();
      handleSpanOnSuccess(span);
      resolve(result);
    } catch (error) {
      handleSpanOnError(span, error);
      reject(error);
    }
  });
};

export const handleSpanOnSuccess = (span?: Span): void => {
  if (span === undefined) {
    return;
  }

  span.setStatus({ code: SpanStatusCode.OK });
  span.end();
};

export const handleSpanOnError = (span?: Span, error?: unknown): void => {
  if (span === undefined) {
    return;
  }

  span.setStatus({ code: SpanStatusCode.ERROR });

  if (error instanceof Error) {
    let exitCode: number | undefined = undefined;
    if (error instanceof ErrorWithExitCode) {
      exitCode = error.exitCode;
    }

    const { message, name, stack } = error;
    span.recordException({ code: exitCode, message, name, stack });
  }

  span.end();
};
