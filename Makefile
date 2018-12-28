ASSEMBLY_JAR = target/scala-2.11/kamu-ingest-polling-assembly-0.0.1.jar
SPARK_SUBMIT_OPTS = --class IngestApp --master=local[4]
REMOTE_DEBUG_OPTS = -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005


.PHONY: run
run:
	sbt assembly

	spark-submit \
		$(SPARK_SUBMIT_OPTS) \
		$(ASSEMBLY_JAR)


.PHONY: debug
debug:
	sbt assembly

	SPARK_SUBMIT_OPTS=$(REMOTE_DEBUG_OPTS) \
	spark-submit \
		$(SPARK_SUBMIT_OPTS) \
		$(ASSEMBLY_JAR)
