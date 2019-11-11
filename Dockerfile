FROM openjdk:11
ARG VERSION_HASH="SNAPSHOT"
ENV VERSION_HASH=$VERSION_HASH
COPY target/scala-2.12/consumer-prefetch.jar /consumer.jar
COPY run.sh /run.sh
ENTRYPOINT ["run.sh"]