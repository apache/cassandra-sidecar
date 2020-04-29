FROM ubuntu:bionic
RUN apt update && apt install -y openjdk-11-jdk-headless
RUN mkdir /downloads
WORKDIR /downloads/
COPY apache-cassandra-4.0-beta1-bin.tar.gz .
RUN tar zxvf apache-cassandra-4.0-beta1-bin.tar.gz
RUN mv apache-cassandra-4.0-beta1 /cassandra

ADD docker-entrypoint.sh /cassandra/
RUN chmod +x /cassandra/docker-entrypoint.sh

ADD optimize-memory.sh .

RUN chmod +x optimize-memory.sh && ./optimize-memory.sh

EXPOSE 9042
ENTRYPOINT ["/cassandra/docker-entrypoint.sh"]

