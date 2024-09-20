#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
# Stop on first test failing
set -e
# Each test individually
LD_LIBRARY_PATH=${SCRIPT_DIR}/../build python ${SCRIPT_DIR}/tests.py ${SCRIPT_DIR}/to_mqtt.yml test_to_mqtt
LD_LIBRARY_PATH=${SCRIPT_DIR}/../build python ${SCRIPT_DIR}/tests.py ${SCRIPT_DIR}/to_mqtt_event.yml test_to_mqtt_event
LD_LIBRARY_PATH=${SCRIPT_DIR}/../build python ${SCRIPT_DIR}/tests.py ${SCRIPT_DIR}/from_mqtt.yml test_from_mqtt
LD_LIBRARY_PATH=${SCRIPT_DIR}/../build python ${SCRIPT_DIR}/tests.py ${SCRIPT_DIR}/from_mqtt_events.yml test_from_mqtt_events
# All tests in one
LD_LIBRARY_PATH=${SCRIPT_DIR}/../build python ${SCRIPT_DIR}/tests.py ${SCRIPT_DIR}/test_config.yml
echo "ALL TESTS PASSED"