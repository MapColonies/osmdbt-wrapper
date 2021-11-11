# osmdbt-wrapper
a cronjob wrapper for [osmdbt](https://github.com/openstreetmap/osmdbt) used for creating and uploading replications from OSM database to S3

The replication creation is determined by a `state.txt` file. to initialize the replication place the file in your S3 bucket, this file is the source of truth for the job and will be up for update with each job.

if no changes were found on a job no replications would be uploaded and the state file will remain the same.

Only one job can run at a time to prevent a race condition on the replication slot changes and for the replications to remain linear.

## Configuration

**Values**

- `cronjob.schedule` - the cronjob schedule interval in the format of [the cron schedule syntax](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#cron-schedule-syntax)

- `dbConfig.sslAuth.enabled` - enabling postgres certificate auth
- `dbConfig.sslAuth.secretName` - secret name containing the certificates for `cert-conf` volume
- `dbConfig.sslAuth.mountPath` - the path for the mounted certificates

- `objectStrorageConfig.endpoint` - the full url for the object storage
- `objectStrorageConfig.bucketName` - object storage bucket name
- `objectStrorageConfig.accessKey` - object storage access key id
- `objectStrorageConfig.secretKey` - object storage secret access key
- `objectStrorageConfig.acl` - The Access-Control-List for the uploaded objects, defaults to public-read. [read more](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)

- `osmdbt.replicationSlotName` - replication slot name
- `osmdbt.verbose` - a flag for running osmdbt commnads in verbose mode
- `osmdbt.getLogMaxChanges` - the maximum number of changes in OSM database to be fetched into one log file - meaning will be present in one diff. the actual number might be higher than stated because the actual fetching is till the closest higher commit (closed changeset), e.g. `getLogMaxChanges` is set to 1 and there are 5 changes in the database in the following order: 2 node creations and their commit, 1 node creation and its commit - a total of 5 changes. the number of changes that will be fetched to the log are 3. from the configured `getLogMaxChanges` value which is 1 to the closest higher commit which is 3.

**Exit Codes:**

*Exit codes mapping:*

| Exit Code Number | Name                      | Meaning                                                                         |
|------------------|---------------------------|---------------------------------------------------------------------------------|
| 0                | success                   | the program finished successfuly.                                               |
| 1                | general error             | catchall for general errors.                                                    |
| 100              | osmdbt error              | failure occoured while running an osmdbt command.                               |
| 101              | state fetch error         | failure in fetching the state file from s3.                                     |
| 102              | invalid state error       | state file located in s3 is invalid.                                            |
| 103              | s3 upload error           | s3 upload failed.                                                               |
| 130              | terminated                | the program was terminated by the user.                                         |