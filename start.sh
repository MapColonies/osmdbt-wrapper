#!/usr/bin/env bash

POSTGRES_CERTIFICATES_PATH=/.postgresql

if [ "$POSTGRES_ENABLE_SSL_AUTH" = "true" ]
then
  cp $POSTGRES_CERTS_MOUNT_PATH/* $POSTGRES_CERTIFICATES_PATH
  chmod 400 $POSTGRES_CERTIFICATES_PATH/*.key
fi

dumb-init node --import ./instrumentation.mjs ./index.js