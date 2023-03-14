import { trace as traceAPI, Attributes, Context, Span, SpanStatusCode } from '@opentelemetry/api';
import { ErrorWithExitCode } from '../../errors';

export const TRACER_NAME = 'osmdbt-wrapper';

export const promisifySpan = async <T>(spanName: string, spanAttributes: Attributes, context: Context, fn: () => Promise<T>): Promise<T> => {
  const tracer = traceAPI.getTracer(TRACER_NAME);

  return new Promise((resolve, reject) => {
    const span = tracer.startSpan(spanName, { attributes: spanAttributes }, context);
    fn()
      .then((result) => {
        handleSpanOnSuccess(span);
        resolve(result);
      })
      .catch((error) => {
        handleSpanOnError(span, error);
        reject(error);
      });
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
