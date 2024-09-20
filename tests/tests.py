import os
import json
import time
import signal
import sys
import multiprocessing as mp

import libafb

import paho.mqtt.client as mqtt


OUT_TOPIC = "out_topic"
IN_TOPIC = "in_topic"

mqttc = None

test_verb_called = None

my_event = None


def test_verb_cb(req, data):
    global test_verb_called
    test_verb_called = True
    return (0, {"toto": 42})


def subscribe_verb_cb(req, arg):
    if arg.get("action") == "subscribe":
        libafb.evtsubscribe(req, my_event)
    return 0


def control_cb(api, state):
    global my_event
    if state == "ready":
        my_event = libafb.evtnew(api, "py-event")

    return 0


def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    client.subscribe(OUT_TOPIC)
    pipe = userdata
    pipe.send(("ready",))


def mqtt_publish(data: dict):
    return mqttc.publish(IN_TOPIC, json.dumps(data).encode("utf-8"))


def on_mqtt_message(client, userdata, msg):
    mqtt_p = userdata
    payload = json.loads(msg.payload.decode("utf-8"))
    name = payload.get("name")
    type = payload.get("type")
    id = payload.get("id")
    data = payload.get("data")

    if name == "echo" and type == "request" and msg.topic == OUT_TOPIC:
        mqtt_publish({"id": id, "type": "response", "name": name, "data": data})
    elif name == "test" and type == "response" and msg.topic == OUT_TOPIC:
        mqtt_p.send(("received", data))
    elif type == "update" and msg.topic == OUT_TOPIC:
        mqtt_p.send(("received", data))


def mqtt_main(pipe):
    global mqttc
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.user_data_set(pipe)
    mqttc.on_message = on_mqtt_message
    mqttc.on_connect = on_mqtt_connect

    mqttc.connect("localhost", 1883, 60)

    while True:
        if pipe.poll():
            command, *args = pipe.recv()
            if command == "publish":
                mqtt_publish(args[0])
            elif command == "end":
                break
        mqttc.loop(0.5)


def main():
    global binder

    if len(sys.argv) < 2:
        print("Argument: config_file_path.yaml [test1] [test2] ...")
        sys.exit(-1)

    config_file_path = sys.argv[1]

    tests = sys.argv[2:]

    binder = libafb.binder(
        {
            "uid": "py-binder",
            "port": 0,
            "verbose": 255,
            "rootdir": ".",
            "extensions": [
                {
                    "path": "libafb-mqtt-ext.so",
                    "uid": "mqtt",
                    "config": {"mqtt-config-file": config_file_path},
                }
            ],
        }
    )

    libafb.apiadd(
        {
            "uid": "test",
            "api": "test",
            "verbose": 9,
            "export": "public",
            "control": control_cb,
            "verbs": [
                {
                    "uid": "test",
                    "verb": "test",
                    "callback": test_verb_cb,
                },
                {
                    "uid": "subscribe",
                    "verb": "subscribe",
                    "callback": subscribe_verb_cb,
                },
            ],
        }
    )

    mqtt_p, mqtt_child = mp.Pipe()

    mqtt_process = mp.Process(target=mqtt_main, args=(mqtt_child,))

    def run_tests(handle, mqtt_p):
        t = Tests(mqtt_p)
        return t.run(tests)

    try:
        signal.signal(signal.SIGINT, signal.default_int_handler)
        mqtt_process.start()
        msg, *args = mqtt_p.recv()
        if msg != "ready":
            raise RuntimeError()

        r = libafb.loopstart(binder, run_tests, mqtt_p)

        mqtt_p.send(("end",))

        mqtt_process.join()

        sys.exit(0 if r == 1 else r)

    except KeyboardInterrupt:
        mqtt_process.terminate()


class Tests:
    def __init__(self, mqtt_p):
        global binder
        self.mqtt_p = mqtt_p
        self.binder = binder

    def run(self, expected_tests=None):
        """unittest-like member function selection"""
        available_tests = [f for f in dir(self) if f.startswith("test_")]
        tests = (
            available_tests
            if not expected_tests
            else list(set(available_tests).intersection(set(expected_tests)))
        )
        for test_f in tests:
            foo = getattr(self, test_f)
            print(f"**** {test_f}")
            try:
                foo()
            except Exception as e:
                print("******** Exception", e)
                return -1
            
        # no error
        return 1

    def test_to_mqtt(self):
        # Test "to_mqtt" normal behaviour
        # 1. we call the "to_mqtt" api
        # 2. the extension will translate it to a publication on mqtt
        # 3. since the verb is "echo", our mqtt callback will publish back our data as a response
        for data_to_test in (None, 42, "oki", {"key": "value"}):
            r = libafb.callsync(self.binder, "to_mqtt", "echo", data_to_test)
            assert r.status == 0
            assert r.args[0] == data_to_test

        # Test unexisting verb => timeout
        raised = False
        try:
            r = libafb.callsync(self.binder, "to_mqtt", "blurp", {})
        except RuntimeError as e:
            raised = True
        assert raised

    def test_to_mqtt_event(self):
        # wait for subscription verb to be called on init
        time.sleep(0.5)

        # simulate an event coming
        libafb.evtpush(my_event, "hello")

        # check that the mqtt message has been sent
        cmd, data = self.mqtt_p.recv()
        assert cmd == "received"
        assert data == "hello"

    def test_from_mqtt(self):
        # Test "from_mqtt"
        # 1. we simulate a request from MQTT
        # 2. the extension will translate it to a verb call on the "test" api
        # 3. the test verb is implemented here and always return {"toto": 42} as data
        # 4. the extension send an MQTT message back to the caller
        self.mqtt_p.send(
            (
                "publish",
                {"id": "myid", "type": "request", "name": "test", "data": "toto"},
            )
        )
        cmd, data = self.mqtt_p.recv()
        assert cmd == "received"
        assert data == {"toto": 42}

    def test_from_mqtt_events(self):
        # Test the "event" mode
        raised = False
        try:
            libafb.callsync(self.binder, "from_mqtt", "subscribe")
        except RuntimeError as e:
            assert e.args[0] == "invalid-request"
            raised = True
        assert raised

        for wrong_arg in (None, 42, "string", {"ok": 42}):
            raised = False
            try:
                libafb.callsync(self.binder, "from_mqtt", "subscribe", wrong_arg)
            except RuntimeError as e:
                assert e.args[0] == "invalid-request"
                raised = True
            assert raised

        r = libafb.callsync(self.binder, "from_mqtt", "subscribe", ["event1", "event2"])
        assert r.status == 0

        event_cb_called = {}

        def test_event_cb(event_name: str):
            def test_event_cb_(handler, afb_event_name, userdata, data):
                nonlocal event_cb_called
                assert afb_event_name == f"from_mqtt/event/{event_name}"
                assert data == "toto"
                event_cb_called[event_name] = True

            return test_event_cb_

        for event_name in ("event1", "event2"):
            libafb.evthandler(
                self.binder,
                {
                    "pattern": f"from_mqtt/event/{event_name}",
                    "callback": test_event_cb(event_name),
                },
                None,
            )
            event_cb_called[event_name] = False

            self.mqtt_p.send(
                (
                    "publish",
                    {
                        "id": "myid",
                        "type": "update",
                        "name": event_name,
                        "data": "toto",
                    },
                )
            )
            for _ in range(3):
                if event_cb_called[event_name]:
                    break
                time.sleep(0.5)
            assert event_cb_called[event_name]


if __name__ == "__main__":
    main()
