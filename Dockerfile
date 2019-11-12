FROM openjdk:14
ARG VERSION_HASH="SNAPSHOT"
ENV VERSION_HASH=$VERSION_HASH
RUN mkdir -p /srv
COPY target/scala-2.12/consumer-prefetch.jar /consumer-prefetch.jar
ENTRYPOINT java $JAVA_OPTS -jar /consumer-prefetch.jar start --config /srv/common.conf