FROM openjdk:14
ARG VERSION_HASH="SNAPSHOT"
ENV VERSION_HASH=$VERSION_HASH
RUN mkdir -p /srv
COPY target/scala-2.13/consumer.jar /consumer.jar
#COPY target/netty.jar /netty.jar
ENTRYPOINT java $JAVA_OPTS -cp netty.jar -jar consumer.jar --config /srv/common.conf
