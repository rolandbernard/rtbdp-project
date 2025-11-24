FROM flink:2.1.0-java17
WORKDIR /opt/flink
COPY log4j.properties ./conf/log4j-console.properties
RUN mkdir ./checkpoints ./savepoints ./rocksdb-data ./ha-metadata
RUN chown flink:flink ./checkpoints ./savepoints ./rocksdb-data ./ha-metadata
