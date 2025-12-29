export type Executable = 'osmdbt' | 'osmium';

export enum OsmdbtCommand {
  GET_LOG = 'osmdbt-get-log',
  CREATE_DIFF = 'osmdbt-create-diff',
  CATCHUP = 'osmdbt-catchup',
}

export enum OsmiumCommand {
  FILE_INFO = 'fileinfo',
}

export interface CommandContext {
  executable: Executable;
  command: OsmdbtCommand | OsmiumCommand;
  args: string[];
}
