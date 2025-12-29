import { OsmdbtCommand, OsmiumCommand } from '@src/executables';

export enum ExecutableAttributes {
  EXECUTABLE_COMMAND = 'executable.command',
  EXECUTABLE_COMMAND_ARGS = 'executable.command.args',
}

export enum CommandSpanName {
  CATCHUP = 'osmdbt.catchup',
  CREATE_DIFF = 'osmdbt.create-diff',
  GET_LOG = 'osmdbt.get-log',
  FILE_INFO = 'osmium.fileinfo',
}

export const commandToSpanName = (command: OsmdbtCommand | OsmiumCommand): CommandSpanName => {
  switch (command) {
    case OsmdbtCommand.GET_LOG:
      return CommandSpanName.GET_LOG;
    case OsmdbtCommand.CREATE_DIFF:
      return CommandSpanName.CREATE_DIFF;
    case OsmdbtCommand.CATCHUP:
      return CommandSpanName.CATCHUP;
    case OsmiumCommand.FILE_INFO:
      return CommandSpanName.FILE_INFO;
  }
};
