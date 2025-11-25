# This is a custom Docker image where we create some of the directories for the
# volumes that will be mounted. This is so that they have the correct permissions
# and not the ones for root. Additionally, we also copy in a different log 
# configuration, to be less noisy.

FROM flink:2.1.0-java17
WORKDIR /opt/flink
COPY log4j.properties ./conf/log4j-console.properties
RUN mkdir ./checkpoints ./savepoints ./rocksdb-data ./ha-metadata
RUN chown flink:flink ./checkpoints ./savepoints ./rocksdb-data ./ha-metadata
