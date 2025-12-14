export enum S3SpanName {
  HEAD_OBJECT = 's3.headObject',
  GET_OBJECT = 's3.getObject',
  PUT_OBJECT = 's3.putObject',
  DELETE_OBJECT = 's3.deleteObject',
}

export enum S3Attributes {
  S3_AWS_REGION = 'aws.region',
  S3_BUCKET_NAME = 's3.bucket.name',
  S3_KEY = 's3.key',
  S3_CONTENT_TYPE = 's3.content.type',
  S3_ACL = 's3.acl',
  S3_UPLOAD_COUNT = 's3.upload.count',
  S3_UPLOAD_STATE = 's3.upload.state',
}

export enum S3Method {
  HEAD_OBJECT = 'HeadObject',
  GET_OBJECT = 'GetObject',
  PUT_OBJECT = 'PutObject',
  DELETE_OBJECT = 'DeleteObject',
}
