FROM openjdk:14
ARG VERSION_HASH="SNAPSHOT"
ENV VERSION_HASH=$VERSION_HASH
RUN mkdir -p /srv
COPY target/scala-2.12/consumer.jar /consumer.jar
COPY ~/.ivy2/cache/io.netty/netty-all/jars/netty-all-4.1.48.Final.jar /netty.jar
ENTRYPOINT java $JAVA_OPTS -cp netty.jar -jar consumer.jar start --config /srv/common.conf