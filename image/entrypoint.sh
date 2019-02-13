#!/bin/bash
set -ex


exec $SPARK_HOME/bin/spark-submit \
    --master=local[4] \
    --driver-memory 2G \
    --executor-memory 2G \
    --class IngestApp \
    --driver-java-options "-Dconfig.file=/opt/ingest/config.json" \
    /opt/ingest/ingest-polling-assembly.jar
