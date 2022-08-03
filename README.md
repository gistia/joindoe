# Join Doe

Join Doe is a tool for replicating database contents between environments while deidentifying sensitive data.

It dumps the source data to an S3 bucket, deidentify it and uploads it to the destination.

## Current status

Curerntly the project only works with Redshift.

## How to use

Join Doe executes its jobs from a YAML config file.

Example:

```yaml
source:
  connection_uri: $DATABASE_URL
  tables:
    - name: providers
      transform:
          - column: identifier
            transformer: reverse
          - column: first_name
            transformer: first-name
          - column: last_name
            transformer: last-name
    - name: orders
      transform:
          - column: identifier
            transformer: reverse
store:
  bucket: nw-data-transfer
  aws_access_key_id: $AWS_ACCESS_KEY_ID
  aws_secret_access_key: $AWS_SECRET_ACCESS_KEY
destination:
  connection_uri: $TARGET_DATABASE_URL
```

This config processes two tables from the source database: `providers` and `orders`. It then modifies a couple of fields using a given transformer, stores it on an S3 bucket and then uploads it to the destination database.

The supported transformers can be listed using `joindoe transformers`.

