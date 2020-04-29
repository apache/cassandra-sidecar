#!/usr/bin/env bash

CONF_DIR=/cassandra/conf
YAML=$CONF_DIR/cassandra.yaml


sed -i 's/#MAX_HEAP_SIZE="4G"/MAX_HEAP_SIZE="512m"/' $CONF_DIR/cassandra-env.sh
sed -i 's/#HEAP_NEWSIZE="800M"/HEAP_NEWSIZE="256m"/' $CONF_DIR/cassandra-env.sh
sed -i 's/num_tokens: 256/num_tokens: 1/' $YAML
echo 'phi_convict_threshold: 16' >> $YAML
sed -i 's/concurrent_reads: 32/concurrent_reads: 4/' $YAML
sed -i 's/concurrent_writes: 32/concurrent_writes: 4/' $YAML
sed -i 's/concurrent_counter_writes: 32/concurrent_counter_writes: 4/' $YAML
sed -i 's/# file_cache_size_in_mb: 512/file_cache_size_in_mb: 1/' $YAML

rm -rf /cassandra/data/