FROM openjdk:11
COPY target/scala-2.12/consumer-prefetch.jar /
ENTRYPOINT ["java","-jar","consumer-prefetch.jar"]