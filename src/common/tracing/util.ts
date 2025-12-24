import { trace as traceAPI, Attributes, Context, Span, SpanStatusCode } from '@opentelemetry/api';
import { Counter as PromCounter } from 'prom-client';
import { ErrorWithExitCode } from '../errors';

export const TRACER_NAME = 'osmdbt-wrapper';

export const promisifySpan = async <T>(fn: () => Promise<T>, span: Span): Promise<T> => {
  return new Promise((resolve, reject) => {
    fn()
      .then((result) => {
        handleSpanOnSuccess(span);
        resolve(result);
      })
      .catch((error: unknown) => {
        handleSpanOnError(span, error);
        reject(error instanceof Error ? error : new Error(String(error)));
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

export const handleSpanOnError = (span?: Span, error?: unknown, promCounter: PromCounter | undefined = undefined): void => {
  if (span === undefined) {
    return;
  }
  if (promCounter) {
    promCounter.inc({ rootSpan: span.spanContext().spanId });
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

export const startActivePromisifiedSpan = async <T>(
  spanName: string,
  spanAttributes: Attributes,
  context: Context,
  fn: () => Promise<T>
): Promise<T> => {
  const tracer = traceAPI.getTracer(TRACER_NAME);
  const span = tracer.startSpan(spanName, { attributes: spanAttributes }, context);
  return promisifySpan(fn, span);
};
