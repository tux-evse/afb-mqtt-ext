/*
 * Copyright (C) 2015-2024 IoT.bzh Company
 * Author: Hugo Mercier <hugo.mercier@iot.bzh>
 *
 */

#define _GNU_SOURCE

#include <signal.h>
#include <time.h>

#include <libafb/afb-extension.h>
#include <libafb/afb-misc.h>
#include <libafb/core/afb-data.h>
#include <libafb/core/afb-evt.h>
#include <libafb/core/afb-req-common.h>

#include <rp-utils/rp-jsonc.h>
#include <rp-utils/rp-uuid.h>
#include <rp-utils/rp-yaml.h>

#include <mosquitto.h>

#include "json-utils.h"

// TODO ability to AND filters ?
// TODO rework configuration from MQTT 5 vocabulary

AFB_EXTENSION("MQTT")

/**
 * This API will translate any call to a verb into an MQTT
 * message, according to the supplied configuration.
 */
#define TO_MQTT_API_NAME "to_mqtt"

/**
 * This API exposes two verbs:
 * - subscribe_all: will emit an event "from_mqtt/event" for any MQTT
 *   event message received
 * - subscribe: will emit an event "from_mqtt/event/{event_name}" for a
 *   list of selected MQTT event messages
 */
#define FROM_MQTT_API_NAME "from_mqtt"

#define TIMEOUT_ERROR 1

/**
 * Extractor applied to received messages on a JSON object
 */
struct message_extractor_t
{
    // JSON path to data
    char *data_path;

    // JSON path to verb / event name
    char *verb_path;

    // Optional JSON path filter
    struct json_path_filter_t *filter;
};

void message_extractor_delete(struct message_extractor_t *self)
{
    if (self->filter) {
        json_path_filter_delete(self->filter);
    }
}

/**
 * "to_mqtt" internal structures
 *
 * When a verb is called on the "to_mqtt" API, an MQTT message is sent
 * on a topic. For requests, we expect a response on another MQTT topic.
 * Requests are then added to an internal store so that we are able to
 * match a request if a response arrives.
 */

// Maximum number of requests waiting for a response
#define REQUEST_QUEUE_LEN 32

struct stored_request_t
{
    /** The stored request JSON */
    json_object *json_request;

    /** The AFB request so that we are able to reply */
    struct afb_req_common *afb_req;

    /** The timeout job id, so that we are able to abort it */
    int timeout_job_id;
};

struct to_mqtt_t
{
    int timeout_ms;
    json_object *request_template;
    struct message_extractor_t response_extractor;

    /**
     * Very simple store of requests.
     *
     * It is preallocated. A stored_request_t with a afb_req set to NULL is considered an empty
     * slot.
     *
     */
    struct stored_request_t stored_requests[REQUEST_QUEUE_LEN];

    json_object *on_event_template;

    // array of event_registrations as json array
    json_object *event_registrations;
};

void to_mqtt_delete(struct to_mqtt_t *self)
{
    if (self->request_template)
        json_object_put(self->request_template);
    message_extractor_delete(&self->response_extractor);
    if (self->on_event_template)
        json_object_put(self->on_event_template);
    if (self->event_registrations)
        json_object_put(self->event_registrations);
}

/**
 * Add a request to the internal store.
 *
 * This function looks for an empty slot (in a naive way).
 *
 * @param afb_req The afb request to store
 * @param json The request JSON to store
 * @param timeout_job_id The timeout job id to store
 * @returns -1 on error, 0 otherwise
 */
int to_mqtt_add_stored_request(struct to_mqtt_t *self,
                               struct afb_req_common *afb_req,
                               json_object *json,
                               int timeout_job_id)
{
    int i = 0;
    while ((i < REQUEST_QUEUE_LEN) && (self->stored_requests[i].afb_req))
        i++;

    if (i == REQUEST_QUEUE_LEN) {
        LIBAFB_ALERT("Send queue full");
        return -1;
    }

    self->stored_requests[i].afb_req = afb_req;
    self->stored_requests[i].json_request = json;
    self->stored_requests[i].timeout_job_id = timeout_job_id;
    return i;
}

/**
 * Look for a given afb request in the store
 */
int to_mqtt_get_request_index(struct to_mqtt_t *self, struct afb_req_common *req)
{
    for (size_t i = 0; i < REQUEST_QUEUE_LEN; i++) {
        if (self->stored_requests[i].afb_req == req) {
            return i;
        }
    }
    return -1;
}

/**
 * Given a response, look for the corresponding stored request, if any.
 *
 * @returns the index in the array of stored request if found, -1 if not
 * found
 */
int to_mqtt_match_reponse(struct to_mqtt_t *self, json_object *response)
{
    int i = 0;
    for (; i < REQUEST_QUEUE_LEN; i++) {
        struct stored_request_t *sr = &self->stored_requests[i];
        if (!sr->afb_req)
            // item deleted
            continue;

        //
        // FIXME id matching
        json_object *request_id = json_object_get_path(sr->json_request, ".id");
        if (!request_id)
            continue;
        const char *request_id_str = json_object_get_string(request_id);
        json_object *response_id = json_object_get_path(response, ".id");
        if (!response_id)
            continue;
        const char *response_id_str = json_object_get_string(response_id);
        if (strcmp(response_id_str, request_id_str))
            continue;

        // Filter matching
        if (self->response_extractor.filter) {
            if (json_path_filter_does_apply(self->response_extractor.filter, response)) {
                json_object_put(sr->json_request);
                return i;
            }
        }
    }

    return -1;
}

void to_mqtt_delete_stored_request(struct to_mqtt_t *self, size_t index)
{
    self->stored_requests[index].afb_req = NULL;
}

/**
 * "from mqtt" internal structures
 */
struct from_mqtt_t
{
    // AFB API to call a verb on when we receive a request/event on MQTT
    char *api_name;

    int timeout_ms;

    struct message_extractor_t request_extractor;
    json_object *response_template;

    struct message_extractor_t event_extractor;

    // Event sent whenever an MQTT "event" message is received
    struct afb_evt *unfiltered_event;

    // Map of event name => afb_evt* as a JSON object (afb_evt* stored as an int64_t)
    json_object *subscribed_events;
};

void from_mqtt_delete(struct from_mqtt_t *self)
{
    if (self->response_template)
        json_object_put(self->response_template);
    message_extractor_delete(&self->request_extractor);
    message_extractor_delete(&self->event_extractor);

    if (self->unfiltered_event)
        afb_evt_unref(self->unfiltered_event);
    if (self->subscribed_events) {
        // delete all stored events
        json_object_object_foreach(self->subscribed_events, key, val)
        {
            (void)key;
            afb_evt_unref((struct afb_evt *)json_object_get_int64(val));
        }
        json_object_put(self->subscribed_events);
    }
}

bool from_mqtt_is_request(struct from_mqtt_t *self, json_object *message)
{
    if (self->request_extractor.filter) {
        return json_path_filter_does_apply(self->request_extractor.filter, message);
    }
    // if no filter is defined
    return true;
}

bool from_mqtt_is_event(struct from_mqtt_t *self, json_object *message)
{
    if (self->event_extractor.filter) {
        return json_path_filter_does_apply(self->event_extractor.filter, message);
    }
    // if no filter is defined
    return true;
}

struct mqtt_ext_handler_t
{
    json_object *config_json;
    struct mosquitto *mosq;
    char *broker_host;
    int broker_port;
    char *subscribe_topic;
    char *publish_topic;
    struct to_mqtt_t to_mqtt;
    struct from_mqtt_t from_mqtt;
    struct afb_apiset *call_set;

    struct ev_fd *efd;
};

struct mqtt_ext_handler_t g_handler;

char default_mqtt_broker_host[] = "localhost";
const int default_mqtt_broker_port = 1883;
const int default_mqtt_timeout_ms = 60000;

void mqtt_ext_handler_init(struct mqtt_ext_handler_t *self)
{
    memset(self, 0, sizeof(struct mqtt_ext_handler_t));

    self->broker_host = default_mqtt_broker_host;
    self->broker_port = default_mqtt_broker_port;

    self->to_mqtt.timeout_ms = default_mqtt_timeout_ms;

    afb_evt_create2(&self->from_mqtt.unfiltered_event, FROM_MQTT_API_NAME, "event");
}

void mqtt_ext_handler_delete(struct mqtt_ext_handler_t *self)
{
    if (self->mosq)
        mosquitto_destroy(self->mosq);

    to_mqtt_delete(&self->to_mqtt);
    from_mqtt_delete(&self->from_mqtt);

    json_object_put(self->config_json);

    afb_apiset_unref(self->call_set);
}

/**
 * Response timeout callback.
 *
 * When a verb is called on the "to_mqtt" API, an MQTT message is send.
 * Requests expect a corresponding response on MQTT. When the MQTT
 * message is sent, a timer is started and this callback is called when
 * the timer times out. In this case it means we should reply a timeout
 * error to AFB and remove the request from the internal store.
 */
void on_response_timeout(int signal, void *data)
{
    if (signal)
        return;

    struct afb_req_common *req = (struct afb_req_common *)data;
    int index = to_mqtt_get_request_index(&g_handler.to_mqtt, req);

    if (index == -1) {
        // No request found, it has probably already been responsed to.
        // Do nothing then
        return;
    }

    json_object_put(g_handler.to_mqtt.stored_requests[index].json_request);
    g_handler.to_mqtt.stored_requests[index].afb_req = NULL;

    struct afb_data *reply;
    afb_data_create_raw(&reply, &afb_type_predefined_stringz, "Timeout waiting for response", 0,
                        NULL, NULL);
    afb_req_common_reply(req, TIMEOUT_ERROR, 1, &reply);
    afb_req_common_unref(req);
}

/**
 * "Augmented" afb_req_common structure
 *
 * In the "from_mqtt" direction, allows us to store a request json
 * alongside the request
 */
struct my_req_t
{
    struct afb_req_common req;

    json_object *request_json;
};

/**
 * Called in the "from_mqtt" direction when an afb verb has been called
 * following an MQTT message reception.
 *
 * It publishes an MQTT response wrapping data replied by the afb verb
 */
void on_verb_call_reply(struct afb_req_common *req,
                        int status,
                        unsigned nreplies,
                        struct afb_data *const replies[])
{
    struct my_req_t *my_req = containerof(struct my_req_t, req, req);

    if (status) {
        // TODO error message
        return;
    }

    json_object *mapping = json_object_new_object();
    json_object_object_add(mapping, "request", json_object_get(my_req->request_json));
    json_object_object_add(mapping, "verb", json_object_new_string(req->verbname));

    json_object *reply_data = afb_data_ro_pointer(replies[0]);
    json_object_object_add(mapping, "data", json_object_get(reply_data));
    json_object *filled = json_object_fill_template(g_handler.from_mqtt.response_template, mapping);
    json_object_put(mapping);
    const char *filled_str = json_object_get_string(filled);

    mosquitto_publish(g_handler.mosq, /* mid = */ NULL, g_handler.publish_topic, strlen(filled_str),
                      filled_str,
                      /* qos = */ 0, /* retain = */ false);

    json_object_put(filled);
}

void on_my_req_unref(struct afb_req_common *req)
{
    struct my_req_t *my_req = containerof(struct my_req_t, req, req);
    json_object_put(my_req->request_json);
    free(my_req);
}

void on_verb_call_no_reply(struct afb_req_common *req,
                           int status,
                           unsigned nreplies,
                           struct afb_data *const replies[])
{
}

struct afb_req_common_query_itf verb_call_itf = {.reply = on_verb_call_reply,
                                                 .unref = on_my_req_unref};

/**
 * MQTT subscription callback
 */
void on_mqtt_message(struct mosquitto *mosq, void *user_data, const struct mosquitto_message *msg)
{
    // Parse the received message as JSON
    json_tokener *tokener = json_tokener_new();
    json_object *mqtt_json = json_tokener_parse_ex(tokener, msg->payload, msg->payloadlen);
    json_tokener_free(tokener);

    if (!mqtt_json) {
        // Not a valid JSON, abort
        LIBAFB_NOTICE("Received MQTT message is not valid JSON");
        return;
    }

    int request_idx;

    if (from_mqtt_is_request(&g_handler.from_mqtt, mqtt_json)) {
        json_object *verb =
            json_object_get_path(mqtt_json, g_handler.from_mqtt.request_extractor.verb_path);
        const char *verb_str = verb ? json_object_get_string(verb) : NULL;
        json_object *data =
            json_object_get_path(mqtt_json, g_handler.from_mqtt.request_extractor.data_path);
        data = data ? json_object_get(data) : json_object_new_null();

        struct my_req_t *my_req = malloc(sizeof(struct my_req_t));
        my_req->request_json = json_object_get(mqtt_json);

        struct afb_data *reply;
        afb_data_create_raw(&reply, &afb_type_predefined_json_c, data, 0,
                            (void *)json_object_put, data);
        afb_req_common_init(&my_req->req, /* afb_req_common_query_itf = */ &verb_call_itf,
                            g_handler.from_mqtt.api_name, verb_str, 1, &reply, NULL);
        afb_req_common_process(&my_req->req, g_handler.call_set);

        json_object_put(mqtt_json);
    }
    else if (from_mqtt_is_event(&g_handler.from_mqtt, mqtt_json)) {
        json_object *event =
            json_object_get_path(mqtt_json, g_handler.from_mqtt.event_extractor.verb_path);
        const char *event_name = event ? json_object_get_string(event) : NULL;
        json_object *data =
            json_object_get_path(mqtt_json, g_handler.from_mqtt.event_extractor.data_path);
        data = data ? json_object_get(data) : json_object_new_null();

        struct afb_data *event_data[2];

        afb_data_create_copy(&event_data[0], &afb_type_predefined_stringz, event_name,
                             strlen(event_name) + 1);
        afb_data_create_raw(&event_data[1], &afb_type_predefined_json_c, data, 0,
                            (void *)json_object_put, data);

        // push the "unfiltered" event to all listeners
        afb_evt_push(g_handler.from_mqtt.unfiltered_event, 2, event_data);

        // also push to listeners who have explicitely select this event
        json_object *subscribed_events = g_handler.from_mqtt.subscribed_events;
        json_object *found_json = NULL;
        if (json_object_object_get_ex(subscribed_events, event_name, &found_json)) {
            struct afb_data *event_data;
            data = json_object_get(data);
            afb_data_create_raw(&event_data, &afb_type_predefined_json_c, data, 0,
                                (void *)json_object_put, data);
            struct afb_evt *evt = (struct afb_evt *)json_object_get_int64(found_json);
            afb_evt_push(evt, 1, &event_data);
        }

        json_object_put(mqtt_json);
    }
    else if ((request_idx = to_mqtt_match_reponse(&g_handler.to_mqtt, mqtt_json)) != -1) {
        struct stored_request_t *stored_request = &g_handler.to_mqtt.stored_requests[request_idx];

        // disarm the response timeout
        afb_sched_abort_job(stored_request->timeout_job_id);

        // extract useful data from response
        json_object *response_json = mqtt_json;
        if (g_handler.to_mqtt.response_extractor.data_path) {
            response_json =
                json_object_get_path(mqtt_json, g_handler.to_mqtt.response_extractor.data_path);
            response_json = response_json ? json_object_get(response_json) : json_object_new_null();
        }

        // craft a reply and reply
        struct afb_data *reply;
        afb_data_create_raw(&reply, &afb_type_predefined_json_c, response_json, 0,
                            (void *)json_object_put, response_json);
        afb_req_common_reply(stored_request->afb_req, 0, 1, &reply);

        // decref the request
        afb_req_common_unref(stored_request->afb_req);

        // do not wait for a response to this request anymore
        to_mqtt_delete_stored_request(&g_handler.to_mqtt, request_idx);

        json_object_put(mqtt_json);
    }
}

/**
 * Utilitary function that can be used in a JSON template
 */
json_object *make_uuid()
{
    rp_uuid_stringz_t uuid;
    rp_uuid_new_stringz(uuid);
    return json_object_new_string(uuid);
}

struct template_function_t id_functions[] = {{.function_name = "uuid", .generator = make_uuid},
                                             {.function_name = NULL, .generator = NULL}};

/**
 * Called when a verb is called on the "to_mqtt" API
 */
static void on_to_mqtt_request(void *closure, struct afb_req_common *req)
{
    struct afb_data *arg_json = NULL;
    int rc = afb_req_common_param_convert(req, 0, &afb_type_predefined_json_c, &arg_json);
    if (rc < 0) {
        LIBAFB_NOTICE("Cannot convert argument to JSON");
        return;
    }
    json_object *arg = afb_data_ro_pointer(arg_json);

    if (g_handler.publish_topic) {
        if (!g_handler.to_mqtt.request_template) {
            LIBAFB_ERROR("No request template defined");
            return;
        }

        json_object *mapping = json_object_new_object();
        json_object_object_add(mapping, "verb", json_object_new_string(req->verbname));
        json_object_object_add(mapping, "data", arg);

        json_object *filled = json_object_fill_template_with_functions(
            g_handler.to_mqtt.request_template, mapping, id_functions);
        const char *request_str = json_object_get_string(filled);

        mosquitto_publish(g_handler.mosq, /* mid = */ NULL, g_handler.publish_topic,
                          strlen(request_str), request_str,
                          /* qos = */ 0, /* retain = */ false);

        req = afb_req_common_addref(req);
        // start a timeout job
        int job_id =
            afb_sched_post_job(/* group = */ NULL, /* delayms = */
                               g_handler.to_mqtt.timeout_ms,
                               /* timeout = */ 0, on_response_timeout, req, Afb_Sched_Mode_Normal);

        to_mqtt_add_stored_request(&g_handler.to_mqtt, req, filled, job_id);
    }
}

static struct afb_api_itf to_mqtt_api_itf = {.process = on_to_mqtt_request};

/**
 * Called when a verb is called on the "from_mqtt" API
 */
static void on_from_mqtt_api_call(void *closure, struct afb_req_common *req)
{
    if (!strcmp(req->verbname, "subscribe_all")) {
        // subscribe to all possible MQTT event messages
        afb_req_common_subscribe(req, g_handler.from_mqtt.unfiltered_event);
        afb_req_common_reply(req, 0, 0, NULL);
        return;
    }

    if (!strcmp(req->verbname, "subscribe")) {
        // subscribe to a list of MQTT event messages
        if (req->params.ndata != 1) {
            // TODO add error message
            afb_req_common_reply(req, AFB_ERRNO_INVALID_REQUEST, 0, NULL);
            return;
        }
        const struct afb_type *type = afb_data_type(req->params.data[0]);
        if (type != &afb_type_predefined_json_c) {
            // TODO add error message
            afb_req_common_reply(req, AFB_ERRNO_INVALID_REQUEST, 0, NULL);
            return;
        }
        json_object *param = afb_data_ro_pointer(req->params.data[0]);
        if (!json_object_is_type(param, json_type_array)) {
            // TODO add error message
            afb_req_common_reply(req, AFB_ERRNO_INVALID_REQUEST, 0, NULL);
            return;
        }

        if (!g_handler.from_mqtt.subscribed_events) {
            g_handler.from_mqtt.subscribed_events = json_object_new_object();
        }

        size_t n = json_object_array_length(param);
        for (size_t i = 0; i < n; i++) {
            const char *event_name = json_object_get_string(json_object_array_get_idx(param, i));
            if (!json_object_object_get(g_handler.from_mqtt.subscribed_events, event_name)) {
                // create the event
                struct afb_evt *evt;
                afb_evt_create2(&evt, "from_mqtt/event", event_name);
                afb_req_common_subscribe(req, evt);
                json_object_object_add(g_handler.from_mqtt.subscribed_events, event_name,
                                       json_object_new_int64((int64_t)evt));
            }
        }
        afb_req_common_reply(req, 0, 0, NULL);
        return;
    }

    afb_req_common_reply(req, AFB_ERRNO_UNKNOWN_VERB, 0, NULL);
}

static struct afb_api_itf from_mqtt_api_itf = {.process = on_from_mqtt_api_call};

struct afb_evt_listener *g_listener;

/**
 * Called when an afb event we are subscribed to is pushed.
 *
 * We publish an MQTT message in response.
 */
void on_event_pushed(void *closure, const struct afb_evt_pushed *event)
{
    // TODO error if not json_c type
    json_object *data = afb_data_ro_pointer(event->data.params[0]);

    json_object *mapping = json_object_new_object();
    json_object_object_add(mapping, "event_name", json_object_new_string(afb_evt_name(event->evt)));
    json_object_object_add(mapping, "data", json_object_get(data));
    json_object *filled = json_object_fill_template_with_functions(
        g_handler.to_mqtt.on_event_template, mapping, id_functions);
    json_object_put(mapping);
    char *filled_str = (char *)json_object_get_string(filled);

    mosquitto_publish(g_handler.mosq, /* mid = */ NULL, g_handler.publish_topic, strlen(filled_str),
                      filled_str,
                      /* qos = */ 0, /* retain = */ false);

    json_object_put(filled);
}

struct afb_evt_itf event_itf = {
    .push = on_event_pushed,
};

int on_subscribe(struct afb_req_common *req, struct afb_evt *event)
{
    afb_evt_listener_add(g_listener, event, 0);
    return 0;
}

void on_req_unref(struct afb_req_common *req)
{
    free(req);
}

struct afb_req_common_query_itf subscription_call_itf = {.reply = on_verb_call_no_reply,
                                                         .unref = on_req_unref,
                                                         .subscribe = on_subscribe};

/**
 * Called at the initialization of the extension to register a list of
 * events.
 *
 * A list of api/verb are called to subscribe to events.
 */
void init_event_registrations()
{
    // Add a global event listener
    g_listener = afb_evt_listener_create(&event_itf, NULL, NULL);

    // Call event subscriptions, if any
    if (g_handler.to_mqtt.event_registrations) {
        size_t len = json_object_array_length(g_handler.to_mqtt.event_registrations);
        for (size_t i = 0; i < len; i++) {
            char *api = NULL, *verb = NULL, *event_pattern = NULL;
            json_object *args = NULL;
            json_object *registration =
                json_object_array_get_idx(g_handler.to_mqtt.event_registrations, i);
            rp_jsonc_unpack(registration, "{s?s s?s s?O s?s}",  //
                            "api", &api,                        //
                            "verb", &verb,                      //
                            "args", &args,                      //
                            "event", &event_pattern             //
            );

            // Call the verb
            struct afb_data *afb_arg;
            afb_data_create_raw(&afb_arg, &afb_type_predefined_json_c, args, 0,
                                (void *)json_object_put, args);
            struct afb_req_common *req = calloc(1, sizeof(struct afb_req_common));
            afb_req_common_init(req,
                                /* afb_req_common_query_itf = */ &subscription_call_itf,  //
                                api, verb, 1, &afb_arg, NULL);
            afb_req_common_process(req, g_handler.call_set);
        }
    }
}

int parse_config(json_object *config)
{
    mqtt_ext_handler_init(&g_handler);

    json_object *config_file = NULL;
    int found = json_object_object_get_ex(config, "mqtt-config-file", &config_file);
    if (found) {
        const char *config_file_path = json_object_get_string(config_file);
        FILE *fp = fopen(config_file_path, "r");
        if (!fp) {
            LIBAFB_ERROR("[%s] Cannot open configuration file", AfbExtensionManifest.name);
            return -1;
        }

        json_object *config_file_json = NULL;
        int rc = rp_yaml_file_to_json_c(&config_file_json, fp, config_file_path);
        if (rc < 0) {
            fclose(fp);
            LIBAFB_ERROR("[%s] Error parsing YAML configuration file", AfbExtensionManifest.name);
            return -1;
        }

        // store the config json so that strings can be extracted and
        // live as long as this object lives
        g_handler.config_json = config_file_json;

        char *mapping_type = NULL;
        json_object *to_mqtt_json = NULL, *from_mqtt_json = NULL;

        rp_jsonc_unpack(config_file_json, "{s?s s?i s?s s?s s?s s?o s?o}",  //
                        "broker-host", &g_handler.broker_host,              //
                        "broker-port", &g_handler.broker_port,              //
                        "mapping-type", &mapping_type,                      //
                        "subscribe-topic", &g_handler.subscribe_topic,      //
                        "publish-topic", &g_handler.publish_topic,          //
                        "to-mqtt", &to_mqtt_json,                           //
                        "from-mqtt", &from_mqtt_json                        //
        );

        if (strcmp(mapping_type, "topic-pair")) {
            LIBAFB_ERROR("[%s] Unsupported mapping type", AfbExtensionManifest.name);
            return -1;
        }

        if (to_mqtt_json) {
            json_object *extraction = NULL, *event_config = NULL;

            rp_jsonc_unpack(to_mqtt_json, "{s?i s?O s?o s?o}",                        //
                            "timeout-ms", &g_handler.to_mqtt.timeout_ms,              //
                            "request-template", &g_handler.to_mqtt.request_template,  //
                            "response-extraction", &extraction,                       //
                            "event-config", &event_config                             //
            );

            if (extraction) {
                json_object *filter_json = NULL;

                rp_jsonc_unpack(extraction, "{s?s s?o}",  //
                                "data-path",
                                &g_handler.to_mqtt.response_extractor.data_path,  //
                                "filter", &filter_json                            //
                );

                if (filter_json)
                    g_handler.to_mqtt.response_extractor.filter =
                        json_path_filter_from_json_config(filter_json);
            }
            if (event_config) {
                rp_jsonc_unpack(event_config, "{s?O s?O}",                               //
                                "template", &g_handler.to_mqtt.on_event_template,        //
                                "registrations", &g_handler.to_mqtt.event_registrations  //
                );
            }
        }

        if (from_mqtt_json) {
            json_object *extraction = NULL, *event_extraction = NULL;

            rp_jsonc_unpack(from_mqtt_json, "{s?i s?s s?O s?o s?o}",                      //
                            "timeout-ms", &g_handler.from_mqtt.timeout_ms,                //
                            "api", &g_handler.from_mqtt.api_name,                         //
                            "response-template", &g_handler.from_mqtt.response_template,  //
                            "request-extraction", &extraction,                            //
                            "event-extraction", &event_extraction                         //
            );
            if (extraction) {
                json_object *filter_json = NULL;

                rp_jsonc_unpack(extraction, "{s?s s?s s?o}",                                    //
                                "data-path", &g_handler.from_mqtt.request_extractor.data_path,  //
                                "verb-path",
                                &g_handler.from_mqtt.request_extractor.verb_path,  //
                                "filter", &filter_json                             //
                );

                if (filter_json)
                    g_handler.from_mqtt.request_extractor.filter =
                        json_path_filter_from_json_config(filter_json);
            }
            if (event_extraction) {
                json_object *filter_json = NULL;

                rp_jsonc_unpack(event_extraction, "{s?s s?s s?o}",                            //
                                "data-path", &g_handler.from_mqtt.event_extractor.data_path,  //
                                "event-name-path", &g_handler.from_mqtt.event_extractor.verb_path,  //
                                "filter", &filter_json                                        //
                );

                if (filter_json)
                    g_handler.from_mqtt.event_extractor.filter =
                        json_path_filter_from_json_config(filter_json);
            }
        }
    }
    return 0;
}

/**
 * AFB extension interface
 */

const struct argp_option AfbExtensionOptionsV1[] = {{.name = "mqtt-config-file",
                                                     .key = 's',
                                                     .arg = "PATH",
                                                     .doc = "Path to a YAML configuration file"},
                                                    {.name = 0, .key = 0, .doc = 0}};

int AfbExtensionConfigV1(void **data, struct json_object *config, const char *uid)
{
    LIBAFB_NOTICE("Extension %s got config %s", AfbExtensionManifest.name,
                  json_object_get_string(config));

    return parse_config(config);
}

int AfbExtensionDeclareV1(void *data, struct afb_apiset *declare_set, struct afb_apiset *call_set)
{
    LIBAFB_NOTICE("Extension %s successfully registered", AfbExtensionManifest.name);

    struct afb_api_item to_mqtt_api_item;
    to_mqtt_api_item.itf = &to_mqtt_api_itf;
    to_mqtt_api_item.group = (const void *)1;
    to_mqtt_api_item.closure = data;

    struct afb_apiset *ds = declare_set;
    ds = afb_apiset_subset_find(ds, "public");

    int rc;
    if ((rc = afb_apiset_add(ds, TO_MQTT_API_NAME, to_mqtt_api_item)) < 0) {
        LIBAFB_ERROR("Error calling afb_apiset_add (to_mqtt): %d\n", rc);
    }

    struct afb_api_item from_mqtt_api_item = {.itf = &from_mqtt_api_itf, .group = NULL};
    if ((rc = afb_apiset_add(ds, FROM_MQTT_API_NAME, from_mqtt_api_item)) < 0) {
        LIBAFB_ERROR("Error calling afb_apiset_add (from_mqtt): %d\n", rc);
    }

    // Register the call set so that we are able to issue verb calls
    g_handler.call_set = afb_apiset_addref(call_set);
    return 0;
}

int AfbExtensionHTTPV1(void *data, struct afb_hsrv *hsrv)
{
    LIBAFB_NOTICE("Extension %s got HTTP", AfbExtensionManifest.name);
    return 0;
}

void internal_mosquitto_loop(struct ev_fd *efd, int fd, uint32_t revents, void *closure)
{
    struct mosquitto *mosq = closure;

    mosquitto_loop_read(mosq, /* UNUSED */ 1);
    if (mosquitto_want_write(mosq)) {
        mosquitto_loop_write(mosq, /* UNUSED */ 1);
    }
    mosquitto_loop_misc(mosq);
}

int AfbExtensionServeV1(void *data, struct afb_apiset *call_set)
{
    if (mosquitto_lib_init() != MOSQ_ERR_SUCCESS) {
        LIBAFB_ERROR("Error calling mosquitto_lib_init");
        return -1;
    }

    struct mosquitto *mosq =
        mosquitto_new("afb_mqtt_client", /* clean_session = */ true, /* void *obj = */ NULL);
    if (mosq == NULL) {
        LIBAFB_ERROR("Error calling afb_mqtt_client\n");
        return -1;
    }

    g_handler.mosq = mosq;

    int rc = mosquitto_connect(mosq, g_handler.broker_host, g_handler.broker_port,
                               /* keepalive = */ 5);
    if (rc != MOSQ_ERR_SUCCESS) {
        LIBAFB_ERROR("Error on connect: %s", mosquitto_strerror(rc));
        return -1;
    }

    if (g_handler.subscribe_topic) {
        rc = mosquitto_subscribe(mosq, NULL, g_handler.subscribe_topic, /* qos = */ 0);
        if (rc != MOSQ_ERR_SUCCESS) {
            LIBAFB_ERROR("Cannot connect to %s: %s", g_handler.subscribe_topic,
                         mosquitto_strerror(rc));
            return -1;
        }
    }

    mosquitto_message_callback_set(mosq, on_mqtt_message);

    // Instead of mosquitto_loop_start() that would start a new thread,
    // we insert the mosquitto loop in the main AFB event loop

    mosquitto_threaded_set(mosq, true);
    int mosquitto_socket_fd = mosquitto_socket(mosq);

    // struct ev_fd *efd = NULL;
    afb_ev_mgr_add_fd(&g_handler.efd, mosquitto_socket_fd, EPOLLIN | EPOLLOUT,
                      internal_mosquitto_loop,
                      /* closure = */ mosq, /* autounref = */ 1, /* autoclose = */ 1);

    // Call event registration verbs, if needed
    init_event_registrations();

    LIBAFB_NOTICE("Extension %s ready to serve", AfbExtensionManifest.name);
    return 0;
}

int AfbExtensionExitV1(void *data, struct afb_apiset *declare_set)
{
    LIBAFB_NOTICE("Extension %s got to exit", AfbExtensionManifest.name);

    afb_ev_mgr_prepare_wait_dispatch_release(1000);

    mqtt_ext_handler_delete(&g_handler);

    afb_evt_listener_unref(g_listener);

    return 0;
}
