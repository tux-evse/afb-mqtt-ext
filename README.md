# MQTT AFB extension

This AFB extension allows its users to connect two different software buses: the AFB microservice bus and MQTT.

[AFB](https://docs.redpesk.bzh/docs/en/master/redpesk-os/afb-binder/afb-overview.html) is an application framework able to connect microservices. Services expose them through API to the other services and they communicate on the network through function calls (called "verbs") or via signals (called "events").

[MQTT](https://mqtt.org/) is a standard and network protocol for IoT. It is mainly a [publish-subscribe](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) system initially aimed at publishing states of variables to a network of subscribers.

In its [version 5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html), MQTT introduces a request-response model, in addition to the publish-subscribe model.

The goal of this extension is to translate, as transparently as possible for the user, concepts from both worlds of MQTT and AFB and allow one to communicate in both direction.

## Concept mapping

This extension considers two means of communication between AFB and MQTT:
- request-response: an AFB verb call corresponds to an MQTT request messages with parameters and a response message with associated data
- events: an AFB event is similar to a request carrying data, without corresponding response

**This extension assumes payload of MQTT messages are JSON objects.** No other format are supported for now.

MQTT messages are typed as **request**, **response** or **event** based on their content.

## Extension configuration

The extension expects a YAML configuration file that allows one to define how requests and responses are extracted from messages or built.

The extension option `mqtt-config-file` allows the caller to specify the path to such a configuration file.

### General configuration keys

#### broker-host

This key allows one to specify the hostname or IP address of the MQTT broker

*Default value*: `localhost`

#### broker-port

This key allows one to specify the TCP port of the MQTT broker

*Default value*: `1883`

#### mapping-type

This key allows one to specify which mapping between AFB and MQTT is done.

The only only supported so far is `topic-pair`. With this type of mapping, all requests are sent on a given MQTT topic and all responses are also expected on a specific MQTT topic. The correlation between requests and responses are made by extracting data from JSON messages.

*Default value*: `topic-pair`

#### publish-topic

Name of the MQTT topic on which messages are sent.

**Mandatory**

#### subscribe-topic

Name of the MQTT topic on which messages are received

*Default value*: same as the publish topic

## Communication initiated by AFB to MQTT

The extension exposes an API named `to_mqtt`.

Any verb call on this API will send an MQTT message on a specific topic and expects another message interpreted as a response on another topic.

### Configuration

The configuration key `to_mqtt` contains configuration options specific to the AFB -> MQTT communication part.

This item contains the following keys.

#### request-template

The value of this key is used as a template to craft JSON messages for requests. See [JSON templating](#json_templating) for details.

The template will be filled with the following context:
- `${verb}` : name of the verb the `to_mqtt` API has been called with
- `${data}` : first argument of the verb called, as JSON

#### response-extraction

This setting allows one to define how responses are extracted from the stream of MQTT messages on the subscription topic and how they are matched to a request.
It is made of different parts:

- **correlation-path**: The [JSON path](#json_path) to the part of the response used to match the request correlation data. Optional.
- **data-path**: The [JSON path](#json_path) to the part of the response that will constitute the data sent as a reply to the initiating AFB verb call. **Mandatory**.
- **timeout-ms**: Timeout in milliseconds for replies. *Default value*: `60000`.
- **filter**: Optional [message filter](#json_filter).

**Mandatory if `request-template` is defined**

#### request-correlation-path

The [JSON path](#json_path) to the part of the request that will be used to match responses with the request.

**Mandatory if `response-extraction.correlation-path` is defined**

#### event

The extension is able to send "event" messages when AFB events are received.

Upon startup, the extension will call a supplied list of API / verbs that should allow it to register to different events.

When an MQTT message is received, the event extraction will be applied and if the optional filter matches, it will emit an event.

The sub-parts of the configuration are:
- **registrations**: an array of API / verb / arguments to call on startup. Each element of the array must contain
  - **api**: the name of the API to call
  - **verb**: the name of the verb to call
  - **args**: JSON sub tree to send as first argument to the verb
- **template**: [JSON template](#json_template) of the MQTT message to send when an event is received. The context of the template fill is the following:
  - `${event_name}` : name of the received event
  - `${data}`: JSON data associated with the event

### Examples


#### Request/response

```
broker-host: localhost
broker-port: 1883
mapping-type: topic-pair
publish-topic: out_topic
subscribe-topic: in_topic

to-mqtt:
  timeout-ms: 500
  request-template:
    id: ${uuid()}
    name: ${verb}
    type: request
    data: ${data}
  request-correlation-path: .id
  response-extraction:
    filter:
      path: .type
      value: response
    data-path: .data
    correlation-path: .id
```

With this example configuration, when the API `to_mqtt` is called with the verb `my_verb` and `{"ok": 42}` as data, the extension will:
- send the MQTT message `{"id": "b7644388-f322-47a5-9179-2a4845883bfc", "name": "my_verb", "type": "request", "data": {"ok": 42}}` on the MQTT topic `out_topic`
- a response MQTT message will be expected on the `in_topic` MQTT topic for 500 ms. The payload of this message must be parsable as JSON as the `.id` key of the JSON message must have the same value as the request correlation data (i.e. `"b7644388-f322-47a5-9179-2a4845883bfc"` here)
- the response should also contain a key `"type"` with `"response` as value
- the key `data` will be used as a reply to the `to_mqtt/my_verb` caller

#### Event

```
publish-topic: out_topic
to-mqtt:
  event:
    registrations:
      # verbs to call to subscribe to events
      - api: test
        verb: subscribe
        args:
          action: subscribe
    template:
      name: ${event_name}
      type: update
      data: ${data}
```

With this example, the extension will call the verb `subscribe` of the api `test` with arguments `{"action": "subscribe"}` on startup.

For all events of this subscription, it will publish an MQTT message on the topic `out_topic` using the supplied template.

  
## Communication initiated by MQTT to AFB

The extension can be configured to react to some MQTT messages and call AFB verbs or emit events.

### Configuration

The `from_mqtt` sub configuration allows one to define settings specific to the MQTT -> AFB direction.
It contains the following sub-parts.

#### api

The api on which to call verbs or emit events in response to MQTT messages.

**Mandatory**.

#### request-extraction

This setting describes how requests are extracted from MQTT messages. It contains the following sub-parts.

- **verb-path**: [JSON path](#json_path) of a string in the MQTT message that will be used as an AFB verb name. **Mandatory**.
- **data-path**: [JSON path](#json_path) of a sub JSON tree in the MQTT message that will be used as first JSON argument of the AFB verb call. **Mandatory**.
- **filter**: An optional [JSON filter](#json_filter) to filter messages

#### response-template

After an AFB verb is called thanks to the request extraction configuration, the extension will wait for the reply and send back an MQTT response wrapping the reply.

This setting key is the [JSON template](#json_template) used to craft an MQTT response.

**Mandatory** if `request-extraction` is defined.

#### event-extraction

The extension is able to emit an event to clients each time a certain MQTT pattern of messages is received.

This requires clients to first subscribe to such events.
For that purpose, the extension exposes a `from_mqtt` API with the following verbs:
- **subscribe_all**: the client calling this verb will be subscribed to all upcoming events from MQTT. The pushed event is named `from_mqtt/event` and contains the event name as its first argument and the data as its second argument
- **subscribe**: this verb allows clients to subscribe to some choosen upcoming events from MQTT. Names of selected events are supplied as strings in an array as first argument of the verb. Each event is named `from_mqtt/event/{event_name}` and the data of the event are the data of the MQTT event message.

The `event-extraction` configuration contains the following keys:
- **event-name-part**: [JSON path](#json_path) in the MQTT message to a string used as the event name. **Mandatory**
- **data-path**: [JSON path](#json_path) in the MQTT message to data. **Mandatory**
- **filter**: Optional message [JSON filter](#json_filter)

### Examples

#### Request/Response

```
publish-topic: out_topic
subscribe-topic: in_topic

from-mqtt:
  # the AFB API to call on requests
  api: test
  request-extraction:
    verb-path: .name
    data-path: .data
    filter:
      path: .type
      value: request

  response-template:
    id: ${request.id}
    name: ${verb}
    type: response
    data: ${data}
```

#### Events

```
subscribe-topic: in_topic

from-mqtt:
  # the AFB API to call on requests
  api: test

  # this will emit an event
  event-extraction:
    event-name-path: .name
    data-path: .data
    filter:
      path: .type
      value: update
```

## Supported JSON template syntax
<a name="json_templating"></a>

A JSON template is a JSON object where strings may have a special syntax and are then substituted by the value of some other variables.

If a string starts with `${` and ends with `}`, the identifier inside is used as a variable name. Depending on which part of the configuration the JSON template is used, the available variables for substitution differ. Refer to the documentation of each configuration section for the list of available variables and their values.

If the variable name ends with `()`, it is interpreted as a function call rather than a fixed value.

If the name of the placeholder contains a dot ('.'), the part before the dot refers to the key in the mapping and the rest to a JSON "path" inside the associated value.

### Supported functions

#### uuid()

This function generates a UUID

## Supported JSON path syntax
<a name="json_path"></a>

Some parts of the configuration use a simplified JSON "path" syntax to specify where to look for a sub part of a JSON tree.

The path is made of object keys separated by a dot (".").

e.g ".child1.child2" will return the part of the JSON object that is
stored as value of the key "child2" of the object stored as value of
the key "child1" of the JSON object.

## Supported JSON filter syntax
<a name="json_filter"></a>

A JSON filter is a string comparison between a part of a JSON tree and a string.

In the configuration, a filter contains the following keys:
- **path**: the JSON path to the part of the JSON tree
- **value**: the string value to compare to
