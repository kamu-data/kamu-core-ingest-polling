ARG BASE_IMAGE
FROM $BASE_IMAGE

ADD image/entrypoint.sh /opt/entrypoint.sh
ADD image/config.json /opt/ingest/config.json
ADD target/scala-2.11/ingest-polling-assembly-*.jar /opt/ingest/ingest-polling-assembly.jar
