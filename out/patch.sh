#!/bin/bash
echo "md5 before:"
md5sum kafka-connect-rabbitmq-1.0.0-preview_patch.jar
cp -R ../bin/io .
jar -uvf kafka-connect-rabbitmq-1.0.0-preview_patch.jar io/confluent/connect/rabbitmq/MessageConverter.class
echo "md5 after"
md5sum kafka-connect-rabbitmq-1.0.0-preview_patch.jar
