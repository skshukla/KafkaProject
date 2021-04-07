#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJ_DIR=${SCRIPT_DIR}/../

CMD="cd ${PROJ_DIR} && mvn -Dtest=com.sachin.work.kafka.cluster.ProducerConsumerCulsterTest#runProducerThreads test"

echo 'Executing Command:->'
echo $CMD
echo ''

/bin/sh -c "$CMD"