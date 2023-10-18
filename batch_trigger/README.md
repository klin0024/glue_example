# Prerequisites

### Create AWS Lambda

### Create S3 Event Notifications
- Prefix: `trigger/`
- Suffix: `.csv`
- Event types: `s3:ObjectCreated:*`
- Destination: `Lambda Function`
- Choose a function

### Create S3 VPC endpoints

### Create Redshift Connectors

### Glue Job Details
- Type: `Spark`
- Glue version: `Glue 4.0`
- Language: `Python 3`
- Maximum number of workers: `2`
- Connections: `Redshift`