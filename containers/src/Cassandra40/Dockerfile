FROM ubuntu:bionic
ARG CASSANDRA_40_VERSION
RUN apt update && apt install -y openjdk-11-jdk-headless
RUN mkdir /downloads
WORKDIR /downloads/
COPY apache-cassandra-$CASSANDRA_40_VERSION-bin.tar.gz .
RUN tar zxvf apache-cassandra-$CASSANDRA_40_VERSION-bin.tar.gz
RUN mv apache-cassandra-$CASSANDRA_40_VERSION /cassandra

ADD docker-entrypoint.sh /cassandra/
RUN chmod +x /cassandra/docker-entrypoint.sh

ADD optimize-memory.sh .

RUN chmod +x optimize-memory.sh && ./optimize-memory.sh

EXPOSE 9042
ENTRYPOINT ["/cassandra/docker-entrypoint.sh"]

