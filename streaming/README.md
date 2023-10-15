# Amazon Kinesis

### Push Messages

```
export data=$(echo '{"admit": "1.0", "gre": "660", "gpa": "22", "rank": "3.0"}'|base64)
aws kinesis put-record --stream-name glue --partition-key glue --data "$data"
```

### Pull Messages

```
aws kinesis get-records --shard-iterator $(aws kinesis get-shard-iterator --stream-name glue --shard-id < ShardId > --shard-iterator-type AT_SEQUENCE_NUMBER --starting-sequence-number < SequenceNumber > |jq .ShardIterator)
```