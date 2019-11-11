FROM openjdk:11
ARG VERSION_HASH="SNAPSHOT"
ENV VERSION_HASH=$VERSION_HASH
RUN mkdir -p /srv
WORKDIR /srv
COPY target/scala-2.12/consumer-prefetch.jar /srv/consumer.jar
ENTRYPOINT ["java","-Dlog-level=DEBUG","-jar","/consumer.jar","start","--config","/srv/common.conf"]