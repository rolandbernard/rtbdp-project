#!/bin/bash
# A small script for running the processor outside of the Flink cluster using
# the mini cluster instead. The JAR file should have been generated beforehand
# using `mvn package`. This is NOT used in the docker container.

# Write class path.
mvn dependency:build-classpath -Dmdep.includeScope=provided -Dmdep.outputFile=.cp.txt

# Run the processor. Note that the `--add-opens` are needed ot the serializer
# throws exceptions.
java \
    --add-opens java.base/java.util=ALL-UNNAMED \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
    --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens java.base/java.io=ALL-UNNAMED \
    --add-opens java.base/java.net=ALL-UNNAMED \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    --add-opens java.base/java.math=ALL-UNNAMED \
    --add-opens java.base/java.time=ALL-UNNAMED \
    --add-opens java.base/java.text=ALL-UNNAMED \
    --add-opens java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED \
    -cp "target/processor.jar:$(cat .cp.txt)" com.rolandb.ProcessorHistory $@
