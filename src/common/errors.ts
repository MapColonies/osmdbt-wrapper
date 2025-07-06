import { ExitCodes } from './constants';

export class ErrorWithExitCode extends Error {
  public constructor(
    message?: string,
    public exitCode: number = ExitCodes.GENERAL_ERROR
  ) {
    super(message);
    this.exitCode = exitCode;
    Object.setPrototypeOf(this, ErrorWithExitCode.prototype);
  }
}
