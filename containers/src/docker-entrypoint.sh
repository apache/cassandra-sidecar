#!/bin/bash
set -e

export CASSANDRA_ADDRESS=$(awk 'END{print $1}' /etc/hosts)

# 4 tokens for faster startup (but still use tokens)
sed -i -e "s/^\(num_tokens:\).*/\1 4 /g" /cassandra/conf/cassandra.yaml
# default listens on localhost, can't connect to that
sed -i -e "s/^\(rpc_address:\).*/\1 ${CASSANDRA_ADDRESS} /g" /cassandra/conf/cassandra.yaml

#echo "-Xmx1G" >> /cassandra/conf/jvm-server.options
#echo "-Xmn500M" >> /cassandra/conf/jvm-server.options

cd /cassandra
bin/cassandra -f -R
