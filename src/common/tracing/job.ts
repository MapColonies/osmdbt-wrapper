export enum SpanName {
  ROOT_JOB = 'root-job',
  PREPARE_ENVIRONMENT = 'prepare-environment',
  PULL_STATE_FILE = 'pull-state-file',
  UPLOAD_DIFF = 'upload-diff',
  COMMIT_CHANGES = 'commit-changes',
  MARK_LOGS = 'mark-logs',
  POST_CATCHUP = 'post-catchup',
  ROLLBACK = 'rollback',
}

export enum JobAttributes {
  JOB_ROLLBACK = 'job.rollback',
  JOB_EXITCODE = 'job.exitcode',
  JOB_STATE_START = 'job.state.start',
  JOB_STATE_END = 'job.state.end',
  JOB_UPLOAD_COUNT = 'job.upload.count',
}
