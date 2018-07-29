#!/bin/bash
echo "md5 before:"
md5sum out/kafka-connect-rabbitmq-1.0.0-preview_patch.jar
jar -uvf out/kafka-connect-rabbitmq-1.0.0-preview_patch.jar bin/io/confluent/connect/rabbitmq/MessageConverter.class
echo "md5 after"
md5sum out/kafka-connect-rabbitmq-1.0.0-preview_patch.jar
