FROM openjdk:8
COPY . /usr/src/textdb
WORKDIR /usr/src/textdb
RUN apt-get update && \
  apt-get install -y maven
RUN mvn clean install -DskipTests
