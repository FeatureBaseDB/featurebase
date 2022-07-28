#!/bin/bash


go install ./cmd/molecula-consumer-kafka
go install ./cmd/kafkaput

export TOPIC=reingesttopic
export SR_URL=schema-registry:8081
export KAFKA=kafka:9092
export PILOSA=pilosa:10101


# put 2 records into Kafka
kafkaput --schema-registry-url=${SR_URL} --kafka-bootstrap-servers=${KAFKA} --subject=blahh --num-partitions=1 --replication-factor=1 --topic=${TOPIC}
kafkaput --schema-registry-url=${SR_URL} --kafka-bootstrap-servers=${KAFKA} --subject=blahh --num-partitions=1 --replication-factor=1 --topic=${TOPIC}


# consume first record
molecula-consumer-kafka -b 1 --max-msgs=1 --kafka-bootstrap-servers=${KAFKA} --topics=${TOPIC} --pilosa-hosts=${PILOSA} --index=reingest_index --id-field=id --schema-registry-url=${SR_URL}

# make sure we consume the second and final record
molecula-consumer-kafka -b 1 --max-msgs=1 --kafka-bootstrap-servers=${KAFKA} --topics=${TOPIC} --pilosa-hosts=${PILOSA} --index=reingest_index --id-field=id --schema-registry-url=${SR_URL} &

# Try to consumer one more record in the background. This should hang... making sure we don't re-consume any records.
molecula-consumer-kafka -b 1 --max-msgs=1 --kafka-bootstrap-servers=${KAFKA} --topics=${TOPIC} --pilosa-hosts=${PILOSA} --index=reingest_index --id-field=id --schema-registry-url=${SR_URL} &

# sleep 10 and then make sure the background job is still running. Will exit 0 if it is, and 1 if not.
sleep 10
jobs | grep Running
