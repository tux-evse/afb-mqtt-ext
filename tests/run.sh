#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
# Stop on first test failing
set -e
# Each test individually
for test in to_mqtt to_mqtt_event from_mqtt from_mqtt_events from_mqtt_events_bcast
do
    LD_LIBRARY_PATH=${SCRIPT_DIR}/../build python ${SCRIPT_DIR}/tests.py ${SCRIPT_DIR}/${test}.yml test_${test}
done
echo "ALL TESTS PASSED"