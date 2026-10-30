FROM confluentinc/cp-kafka-connect:7.7.0

# Install Debezium PostgreSQL connector from Confluent Hub
# cp-kafka-connect already bundles Confluent AvroConverter & Schema Registry client
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:latest
