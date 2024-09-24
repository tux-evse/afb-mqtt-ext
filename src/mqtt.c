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
#include "uthash.h"

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

#define DEFAULT_MQTT_BROKER_HOST "localhost";
#define DEFAULT_MQTT_BROKER_PORT 1883;
#define DEFAULT_MQTT_TIMEOUT_MS  60000;

/**
 * Extractor applied to received messages on a JSON object
 */
struct message_extractor
{
    // JSON path to data
    const char *data_path;

    // JSON path to verb / event name
    const char *verb_path;

    // Optional JSON path filter
    struct json_path_filter *filter;
};

struct message_extractor *message_extractor_new()
{
    return calloc(1, sizeof(struct message_extractor));
}

static void message_extractor_destroy(struct message_extractor *self)
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

struct stored_request
{
    /** The stored request JSON */
    json_object *json_request;

    /** The request correlation data */
    const char *request_correlation_data;

    /** The AFB request so that we are able to reply */
    struct afb_req_common *afb_req;

    /** The timeout job id, so that we are able to abort it */
    int timeout_job_id;
};

struct to_mqtt
{
    int timeout_ms;

    json_object *request_template;
    const char *request_correlation_path;

    struct message_extractor response_extractor;
    const char *response_correlation_path;

    /**
     * Very simple store of requests.
     *
     * It is preallocated. A stored_request_t with a afb_req set to NULL is considered an empty
     * slot.
     *
     */
    struct stored_request stored_requests[REQUEST_QUEUE_LEN];

    json_object *on_event_template;

    // array of event_registrations as json array
    json_object *event_registrations;
};

static struct to_mqtt *to_mqtt_new()
{
    struct to_mqtt *self = calloc(1, sizeof(struct to_mqtt));
    self->timeout_ms = DEFAULT_MQTT_TIMEOUT_MS;
    return self;
}

static void to_mqtt_delete(struct to_mqtt *self)
{
    if (self->request_template)
        json_object_put(self->request_template);
    message_extractor_destroy(&self->response_extractor);
    if (self->on_event_template)
        json_object_put(self->on_event_template);
    if (self->event_registrations)
        json_object_put(self->event_registrations);
    free(self);
}

/**
 * Add a request to the internal store.
 *
 * This function looks for an empty slot (in a naive way).
 *
 * @param self The `to_mqtt` structure to store into
 * @param afb_req The afb request to store
 * @param json The request JSON to store
 * @param timeout_job_id The timeout job id to store
 * @returns -1 on error, 0 otherwise
 */
static int to_mqtt_add_stored_request(struct to_mqtt *self,
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

    struct stored_request *sr = &self->stored_requests[i];

    sr->request_correlation_data = NULL;
    if (self->request_correlation_path) {
        json_object *correlation_item = json_object_get_path(json, self->request_correlation_path);
        if (correlation_item) {
            sr->request_correlation_data = json_object_get_string(correlation_item);
        }
    }

    sr->afb_req = afb_req;
    sr->json_request = json;
    sr->timeout_job_id = timeout_job_id;
    return i;
}

/**
 * Look for a given afb request in the store
 */
static int to_mqtt_get_stored_request_index(struct to_mqtt *self, struct afb_req_common *req)
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
static int to_mqtt_match_reponse(struct to_mqtt *self, json_object *response)
{
    int i = 0;
    for (; i < REQUEST_QUEUE_LEN; i++) {
        struct stored_request *sr = &self->stored_requests[i];
        if (!sr->afb_req)
            // item deleted
            continue;

        // Test correlation data to match the request
        if (sr->request_correlation_data && self->response_correlation_path) {
            json_object *response_correlation_data =
                json_object_get_path(response, self->response_correlation_path);
            if (response_correlation_data) {
                const char *response_cd = json_object_get_string(response_correlation_data);
                if (strcmp(sr->request_correlation_data, response_cd))
                    continue;
            }
        }

        // Additional filter matching
        if (self->response_extractor.filter) {
            if (json_path_filter_does_apply(self->response_extractor.filter, response)) {
                json_object_put(sr->json_request);
                return i;
            }
        }
    }

    return -1;
}

static void to_mqtt_delete_stored_request(struct to_mqtt *self, size_t index)
{
    self->stored_requests[index].afb_req = NULL;
}

/**
 * Internal structure used in a hash map.
 *
 * This hash is used to store a set of afb events that are either explicitely registered by a user,
 * or used for broadcast.
 */
struct registered_event
{
    const char *name; /* key */
    struct afb_evt *evt;

    UT_hash_handle hh;
};

/**
 * "from mqtt" internal structures
 */
struct from_mqtt
{
    // AFB API to call a verb on when we receive a request/event on MQTT
    const char *api_name;

    struct message_extractor *request_extractor;
    json_object *response_template;

    struct message_extractor *event_extractor;

    // Event sent whenever an MQTT "event" message is received
    struct afb_evt *unfiltered_event;

    struct registered_event *registered_events;

    bool broadcast_events;
};

struct from_mqtt *from_mqtt_new()
{
    struct from_mqtt *self = calloc(1, sizeof(struct from_mqtt));
    afb_evt_create2(&self->unfiltered_event, FROM_MQTT_API_NAME, "event");
    return self;
}

static void from_mqtt_delete(struct from_mqtt *self)
{
    if (self->response_template)
        json_object_put(self->response_template);

    if (self->request_extractor) {
        message_extractor_destroy(self->request_extractor);
        free(self->request_extractor);
    }
    if (self->event_extractor) {
        message_extractor_destroy(self->event_extractor);
        free(self->event_extractor);
    }

    if (self->unfiltered_event)
        afb_evt_unref(self->unfiltered_event);

    if (self->registered_events) {
        struct registered_event *s, *tmp;
        /* free the hash table contents */
        HASH_ITER(hh, self->registered_events, s, tmp)
        {
            HASH_DEL(self->registered_events, s);
            free(s);
        }
    }
#if 0
    if (self->subscribed_events) {
        // delete all stored events
        json_object_object_foreach(self->subscribed_events, key, val)
        {
            (void)key;
            afb_evt_unref((struct afb_evt *)json_object_get_int64(val));
        }
        json_object_put(self->subscribed_events);
    }
#endif

    free(self);
}

static bool from_mqtt_is_request(struct from_mqtt *self, json_object *message)
{
    if (!self->request_extractor)
        return false;
    if (self->request_extractor->filter) {
        return json_path_filter_does_apply(self->request_extractor->filter, message);
    }
    // if no filter is defined
    return true;
}

static bool from_mqtt_is_event(struct from_mqtt *self, json_object *message)
{
    if (!self->event_extractor)
        return false;
    if (self->event_extractor->filter) {
        return json_path_filter_does_apply(self->event_extractor->filter, message);
    }
    // if no filter is defined
    return true;
}

struct mqtt_ext_handler
{
    json_object *config_json;
    struct mosquitto *mosq;
    const char *broker_host;
    int broker_port;
    const char *subscribe_topic;
    const char *publish_topic;
    struct to_mqtt *to_mqtt;
    struct from_mqtt *from_mqtt;
    struct afb_apiset *call_set;

    struct ev_fd *efd;
};

static struct mqtt_ext_handler g_handler;

static void mqtt_ext_handler_init(struct mqtt_ext_handler *self)
{
    memset(self, 0, sizeof(struct mqtt_ext_handler));

    self->broker_host = DEFAULT_MQTT_BROKER_HOST;
    self->broker_port = DEFAULT_MQTT_BROKER_PORT;
}

static void mqtt_ext_handler_destroy(struct mqtt_ext_handler *self)
{
    if (self->mosq)
        mosquitto_destroy(self->mosq);

    if (self->to_mqtt)
        to_mqtt_delete(self->to_mqtt);
    if (self->from_mqtt)
        from_mqtt_delete(self->from_mqtt);

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
static void on_response_timeout(int signal, void *data)
{
    if (signal)
        return;

    struct afb_req_common *req = (struct afb_req_common *)data;
    int index = to_mqtt_get_stored_request_index(g_handler.to_mqtt, req);

    if (index == -1) {
        // No request found, it has probably already been responsed to.
        // Do nothing then
        return;
    }

    json_object_put(g_handler.to_mqtt->stored_requests[index].json_request);
    to_mqtt_delete_stored_request(g_handler.to_mqtt, index);

    afb_req_common_reply(req, AFB_ERRNO_TIMEOUT, 0, NULL);
    afb_req_common_unref(req);
}

/**
 * "Augmented" afb_req_common structure
 *
 * In the "from_mqtt" direction, allows us to store a request json
 * alongside the request
 */
struct my_req
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
static void on_verb_call_reply(struct afb_req_common *req,
                               int status,
                               unsigned nreplies,
                               struct afb_data *const replies[])
{
    struct my_req *my_req = containerof(struct my_req, req, req);

    if (status) {
        // TODO error message
        return;
    }

    json_object *mapping = json_object_new_object();
    json_object_object_add(mapping, "request", json_object_get(my_req->request_json));
    json_object_object_add(mapping, "verb", json_object_new_string(req->verbname));

    json_object *reply_data = afb_data_ro_pointer(replies[0]);
    json_object_object_add(mapping, "data", json_object_get(reply_data));
    json_object *filled =
        json_object_fill_template(g_handler.from_mqtt->response_template, mapping);
    json_object_put(mapping);

    size_t filled_len;
    const char *filled_str =
        json_object_to_json_string_length(filled, JSON_C_TO_STRING_PLAIN, &filled_len);

    mosquitto_publish(g_handler.mosq, /* mid = */ NULL, g_handler.publish_topic, filled_len,
                      filled_str,
                      /* qos = */ 0, /* retain = */ false);

    json_object_put(filled);
}

static void on_my_req_unref(struct afb_req_common *req)
{
    struct my_req *my_req = containerof(struct my_req, req, req);
    json_object_put(my_req->request_json);
    free(my_req);
}

static void on_verb_call_no_reply(struct afb_req_common *req,
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
static void on_mqtt_message(struct mosquitto *mosq,
                            void *user_data,
                            const struct mosquitto_message *msg)
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

    LIBAFB_DEBUG("Received MQTT message: %s", json_object_get_string(mqtt_json));

    if (g_handler.from_mqtt && from_mqtt_is_request(g_handler.from_mqtt, mqtt_json)) {
        json_object *verb =
            json_object_get_path(mqtt_json, g_handler.from_mqtt->request_extractor->verb_path);
        if (!verb) {
            json_object_put(mqtt_json);
            LIBAFB_NOTICE("Cannot extract verb name from message through path '%s'",
                          g_handler.from_mqtt->request_extractor->verb_path);
            return;
        }
        const char *verb_str = json_object_get_string(verb);
        json_object *data =
            json_object_get_path(mqtt_json, g_handler.from_mqtt->request_extractor->data_path);
        if (!data) {
            json_object_put(mqtt_json);
            LIBAFB_NOTICE("Cannot extract data from message through path '%s'",
                          g_handler.from_mqtt->request_extractor->data_path);
            return;
        }
        data = json_object_get(data);

        struct my_req *my_req = malloc(sizeof(struct my_req));
        my_req->request_json = mqtt_json;

        struct afb_data *reply;
        afb_data_create_raw(&reply, &afb_type_predefined_json_c, data, 0, (void *)json_object_put,
                            data);
        afb_req_common_init(&my_req->req, /* afb_req_common_query_itf = */ &verb_call_itf,
                            g_handler.from_mqtt->api_name, verb_str, 1, &reply, NULL);
        afb_req_common_process(&my_req->req, g_handler.call_set);
    }
    else if (g_handler.from_mqtt && from_mqtt_is_event(g_handler.from_mqtt, mqtt_json)) {
        json_object *event =
            json_object_get_path(mqtt_json, g_handler.from_mqtt->event_extractor->verb_path);
        if (!event || !json_object_is_type(event, json_type_string)) {
            json_object_put(mqtt_json);
            LIBAFB_NOTICE("Cannot extract event name from message through path '%s'",
                          g_handler.from_mqtt->event_extractor->verb_path);
            return;
        }

        json_object *data =
            json_object_get_path(mqtt_json, g_handler.from_mqtt->event_extractor->data_path);
        if (!data) {
            json_object_put(mqtt_json);
            LIBAFB_NOTICE("Cannot extract data from message through path '%s'",
                          g_handler.from_mqtt->event_extractor->data_path);
            return;
        }
        data = json_object_get(data);

        struct afb_data *event_data[2];

        const char *event_name = json_object_get_string(event);
        size_t event_name_len = json_object_get_string_len(event);

        afb_data_create_copy(&event_data[0], &afb_type_predefined_stringz, event_name,
                             event_name_len + 1);
        afb_data_create_raw(&event_data[1], &afb_type_predefined_json_c, data, 0,
                            (void *)json_object_put, data);

        // push the "unfiltered" event to all listeners
        afb_evt_push(g_handler.from_mqtt->unfiltered_event, 2, event_data);

        // also push to listeners who have explicitely select this event
        struct registered_event *registered_events = g_handler.from_mqtt->registered_events;
        struct afb_evt *evt = NULL;
        struct registered_event *found_event = NULL;
        HASH_FIND_STR(registered_events, event_name, found_event);
        if (found_event) {
            evt = found_event->evt;
        }
        else if (g_handler.from_mqtt->broadcast_events) {
            afb_evt_create2(&evt, "from_mqtt/event", event_name);
            found_event = calloc(1, sizeof(struct registered_event));
            found_event->evt = evt;
            found_event->name = afb_evt_fullname(evt);
            HASH_ADD_KEYPTR(hh, registered_events, event_name, event_name_len, found_event);
        }

        if (evt) {
            struct afb_data *event_data;
            data = json_object_get(data);
            afb_data_create_raw(&event_data, &afb_type_predefined_json_c, data, 0,
                                (void *)json_object_put, data);
            if (g_handler.from_mqtt->broadcast_events)
                afb_evt_broadcast(evt, 1, &event_data);
            else
                afb_evt_push(evt, 1, &event_data);
        }

        json_object_put(mqtt_json);
    }
    else if (g_handler.to_mqtt &&
             (request_idx = to_mqtt_match_reponse(g_handler.to_mqtt, mqtt_json)) != -1) {
        struct stored_request *stored_request = &g_handler.to_mqtt->stored_requests[request_idx];

        // disarm the response timeout
        afb_sched_abort_job(stored_request->timeout_job_id);

        // extract useful data from response
        json_object *response_json =
            json_object_get_path(mqtt_json, g_handler.to_mqtt->response_extractor.data_path);
        response_json = response_json ? json_object_get(response_json) : json_object_new_null();

        // craft a reply and reply
        struct afb_data *reply;
        afb_data_create_raw(&reply, &afb_type_predefined_json_c, response_json, 0,
                            (void *)json_object_put, response_json);
        afb_req_common_reply(stored_request->afb_req, 0, 1, &reply);

        // decref the request
        afb_req_common_unref(stored_request->afb_req);

        // do not wait for a response to this request anymore
        to_mqtt_delete_stored_request(g_handler.to_mqtt, request_idx);

        json_object_put(mqtt_json);
    }
}

/**
 * Utilitary function that can be used in a JSON template
 */
static json_object *make_uuid()
{
    rp_uuid_stringz_t uuid;
    rp_uuid_new_stringz(uuid);
    return json_object_new_string(uuid);
}

struct template_function id_functions[] = {{.function_name = "uuid", .generator = make_uuid},
                                           {.function_name = NULL, .generator = NULL}};

/**
 * Called when a verb is called on the "to_mqtt" API
 */
static void on_to_mqtt_request(void *closure, struct afb_req_common *req)
{
    LIBAFB_DEBUG("Call to to_mqtt/%s", req->verbname);
    struct afb_data *arg_json = NULL;
    int rc = afb_req_common_param_convert(req, 0, &afb_type_predefined_json_c, &arg_json);
    if (rc < 0) {
        LIBAFB_NOTICE("Cannot convert argument to JSON");
        return;
    }
    json_object *arg = afb_data_ro_pointer(arg_json);

    if (g_handler.publish_topic) {
        if (!g_handler.to_mqtt->request_template) {
            LIBAFB_ERROR("No request template defined");
            return;
        }

        json_object *mapping = json_object_new_object();
        json_object_object_add(mapping, "verb", json_object_new_string(req->verbname));
        json_object_object_add(mapping, "data", arg);

        json_object *filled = json_object_fill_template_with_functions(
            g_handler.to_mqtt->request_template, mapping, id_functions);
        size_t filled_len;
        const char *filled_str =
            json_object_to_json_string_length(filled, JSON_C_TO_STRING_PLAIN, &filled_len);

        LIBAFB_DEBUG("Publish on %s: %s", g_handler.publish_topic, filled_str);

        mosquitto_publish(g_handler.mosq, /* mid = */ NULL, g_handler.publish_topic, filled_len,
                          filled_str,
                          /* qos = */ 0, /* retain = */ false);

        req = afb_req_common_addref(req);
        // start a timeout job
        int job_id =
            afb_sched_post_job(/* group = */ NULL, /* delayms = */
                               g_handler.to_mqtt->timeout_ms,
                               /* timeout = */ 0, on_response_timeout, req, Afb_Sched_Mode_Normal);
        if (job_id < 0) {
            afb_req_common_unref(req);
            LIBAFB_ERROR("Error calling afb_sched_job: %d", job_id);
            return;
        }

        if (to_mqtt_add_stored_request(g_handler.to_mqtt, req, filled, job_id) < 0) {
            afb_req_common_unref(req);
            return;
        }
    }
}

static struct afb_api_itf to_mqtt_api_itf = {.process = on_to_mqtt_request};

/**
 * Called when a verb is called on the "from_mqtt" API
 */
static void on_from_mqtt_api_call(void *closure, struct afb_req_common *req)
{
    // In broadcast mode, there is no subcription available
    if (g_handler.from_mqtt->broadcast_events) {
        afb_req_common_reply(req, AFB_ERRNO_UNKNOWN_VERB, 0, NULL);
        return;
    }

    if (!strcmp(req->verbname, "subscribe_all_events")) {
        // subscribe to all possible MQTT event messages
        afb_req_common_subscribe(req, g_handler.from_mqtt->unfiltered_event);
        afb_req_common_reply(req, 0, 0, NULL);
        return;
    }

    if (!strcmp(req->verbname, "subscribe_events")) {
        // subscribe to a list of MQTT event messages
        if (req->params.ndata != 1) {
            afb_req_common_reply(req, AFB_ERRNO_INVALID_REQUEST, 0, NULL);
            return;
        }
        const struct afb_type *type = afb_data_type(req->params.data[0]);
        if (type != &afb_type_predefined_json_c) {
            afb_req_common_reply(req, AFB_ERRNO_INVALID_REQUEST, 0, NULL);
            return;
        }
        json_object *param = afb_data_ro_pointer(req->params.data[0]);
        if (!json_object_is_type(param, json_type_array)) {
            afb_req_common_reply(req, AFB_ERRNO_INVALID_REQUEST, 0, NULL);
            return;
        }

        size_t n = json_object_array_length(param);
        for (size_t i = 0; i < n; i++) {
            json_object *array_item = json_object_array_get_idx(param, i);
            size_t event_name_len = json_object_get_string_len(array_item);
            const char *event_name = json_object_get_string(array_item);
            struct registered_event *found_event = NULL;

            HASH_FIND_STR(g_handler.from_mqtt->registered_events, event_name, found_event);
            if (!found_event) {
                // create the event
                struct afb_evt *evt = NULL;
                afb_evt_create2(&evt, "from_mqtt/event", event_name);
                found_event = calloc(1, sizeof(struct registered_event));
                found_event->name = event_name;
                found_event->evt = evt;
                HASH_ADD_KEYPTR(hh, g_handler.from_mqtt->registered_events, event_name,
                                event_name_len, found_event);
            }
            afb_req_common_subscribe(req, found_event->evt);
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
static void on_event_pushed(void *closure, const struct afb_evt_pushed *event)
{
    LIBAFB_DEBUG("Received event %s", afb_evt_fullname(event->evt));

    struct afb_data *afb_arg = NULL;
    int rc = afb_data_convert(event->data.params[0], &afb_type_predefined_json_c, &afb_arg);
    if (rc < 0) {
        LIBAFB_NOTICE("Cannot convert data to JSON: error code %d", rc);
        return;
    }
    json_object *data = afb_data_ro_pointer(afb_arg);

    json_object *mapping = json_object_new_object();
    json_object_object_add(mapping, "event_name", json_object_new_string(afb_evt_name(event->evt)));
    json_object_object_add(mapping, "data", json_object_get(data));
    json_object *filled = json_object_fill_template_with_functions(
        g_handler.to_mqtt->on_event_template, mapping, id_functions);
    json_object_put(mapping);

    size_t filled_len;
    const char *filled_str =
        json_object_to_json_string_length(filled, JSON_C_TO_STRING_PLAIN, &filled_len);

    mosquitto_publish(g_handler.mosq, /* mid = */ NULL, g_handler.publish_topic, filled_len,
                      filled_str,
                      /* qos = */ 0, /* retain = */ false);

    json_object_put(filled);
}

struct afb_evt_itf event_itf = {
    .push = on_event_pushed,
};

static int on_subscribe(struct afb_req_common *req, struct afb_evt *event)
{
    LIBAFB_DEBUG("Subscribe to event %s", afb_evt_fullname(event));
    afb_evt_listener_add(g_listener, event, 0);
    return 0;
}

static void on_req_unref(struct afb_req_common *req)
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
static void init_event_registrations()
{
    // Add a global event listener
    g_listener = afb_evt_listener_create(&event_itf, NULL, NULL);

    // Call event subscriptions, if any
    size_t len = json_object_array_length(g_handler.to_mqtt->event_registrations);
    for (size_t i = 0; i < len; i++) {
        char *api = NULL, *verb = NULL, *event_pattern = NULL;
        json_object *args = NULL;
        json_object *registration =
            json_object_array_get_idx(g_handler.to_mqtt->event_registrations, i);
        rp_jsonc_unpack(registration, "{s?s s?s s?O s?s}",  //
                        "api", &api,                        //
                        "verb", &verb,                      //
                        "args", &args,                      //
                        "event", &event_pattern             //
        );

        // Call the verb
        struct afb_data *afb_arg;
        afb_data_create_raw(&afb_arg, &afb_type_predefined_json_c, args, 0, (void *)json_object_put,
                            args);
        struct afb_req_common *req = calloc(1, sizeof(struct afb_req_common));

        LIBAFB_DEBUG("Event subscription: call api:%s verb:%s args:%s", api, verb,
                     json_object_get_string(args));
        afb_req_common_init(req,
                            /* afb_req_common_query_itf = */ &subscription_call_itf,  //
                            api, verb, 1, &afb_arg, NULL);
        afb_req_common_process(req, g_handler.call_set);
    }
}

#define MQTT_EXT_ERROR(...) LIBAFB_ERROR("[MQTT] " __VA_ARGS__)

static int parse_config(json_object *config)
{
    mqtt_ext_handler_init(&g_handler);

    json_object *config_file = NULL;
    if (!json_object_object_get_ex(config, "mqtt-config-file", &config_file)) {
        MQTT_EXT_ERROR("No configuration file supplied");
        return -1;
    }

    const char *config_file_path = json_object_get_string(config_file);
    FILE *fp = fopen(config_file_path, "r");
    if (!fp) {
        MQTT_EXT_ERROR("Cannot open configuration file");
        return -1;
    }

    json_object *config_file_json = NULL;
    int rc = rp_yaml_file_to_json_c(&config_file_json, fp, config_file_path);
    fclose(fp);
    if (rc < 0) {
        MQTT_EXT_ERROR("Error parsing YAML configuration file");
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

    if (mapping_type && strcmp(mapping_type, "topic-pair")) {
        MQTT_EXT_ERROR("Unsupported mapping type '%s'", mapping_type);
        return -1;
    }

    if (to_mqtt_json) {
        g_handler.to_mqtt = to_mqtt_new();
        json_object *extraction = NULL, *event_config = NULL;

        rp_jsonc_unpack(to_mqtt_json, "{s?i s?O s?o s?o s?s}",                     //
                        "timeout-ms", &g_handler.to_mqtt->timeout_ms,              //
                        "request-template", &g_handler.to_mqtt->request_template,  //
                        "response-extraction", &extraction,                        //
                        "event", &event_config,                                    //
                        "request-correlation-path",
                        &g_handler.to_mqtt->request_correlation_path  //
        );

        if (g_handler.to_mqtt->request_template && !extraction) {
            MQTT_EXT_ERROR("to_mqtt: Undefined mandatory configuration key 'response-extraction'");
            return -1;
        }

        if (g_handler.to_mqtt->request_template && extraction) {
            json_object *filter_json = NULL;

            rp_jsonc_unpack(extraction, "{s?s s?o s?s}",  //
                            "data-path",
                            &g_handler.to_mqtt->response_extractor.data_path,  //
                            "filter", &filter_json,                            //
                            "correlation-path",
                            &g_handler.to_mqtt->response_correlation_path  //
            );

            if (!g_handler.to_mqtt->response_extractor.data_path) {
                MQTT_EXT_ERROR(
                    "to_mqtt.response-extraction: Undefined mandatory configuration key "
                    "'data-path'");
                return -1;
            }
            if (g_handler.to_mqtt->response_correlation_path &&
                !g_handler.to_mqtt->request_correlation_path) {
                MQTT_EXT_ERROR(
                    "to_mqtt: Undefined mandatory configuration key "
                    "'request-correlation-path'");
                return -1;
            }
            if (filter_json)
                g_handler.to_mqtt->response_extractor.filter =
                    json_path_filter_from_json_config(filter_json);
        }
        if (event_config) {
            rp_jsonc_unpack(event_config, "{s?O s?O}",                                //
                            "template", &g_handler.to_mqtt->on_event_template,        //
                            "registrations", &g_handler.to_mqtt->event_registrations  //
            );
        }
    }

    if (from_mqtt_json) {
        g_handler.from_mqtt = from_mqtt_new();
        json_object *extraction = NULL, *event_extraction = NULL;

        rp_jsonc_unpack(from_mqtt_json, "{s?s s?O s?o s?o s?b}",                       //
                        "api", &g_handler.from_mqtt->api_name,                         //
                        "response-template", &g_handler.from_mqtt->response_template,  //
                        "request-extraction", &extraction,                             //
                        "event-extraction", &event_extraction,                         //
                        "broadcast-events", &g_handler.from_mqtt->broadcast_events     //
        );
        if (extraction) {
            g_handler.from_mqtt->request_extractor = message_extractor_new();
            if (!g_handler.from_mqtt->response_template) {
                MQTT_EXT_ERROR("from_mqtt: 'response-template' missing");
                return -1;
            }
            json_object *filter_json = NULL;

            rp_jsonc_unpack(extraction, "{s?s s?s s?o}",                                      //
                            "data-path", &g_handler.from_mqtt->request_extractor->data_path,  //
                            "verb-path",
                            &g_handler.from_mqtt->request_extractor->verb_path,  //
                            "filter", &filter_json                               //
            );

            if (!g_handler.from_mqtt->request_extractor->data_path) {
                MQTT_EXT_ERROR("from_mqtt.request-extraction: missing 'data-path'");
                return -1;
            }
            if (!g_handler.from_mqtt->request_extractor->verb_path) {
                MQTT_EXT_ERROR("from_mqtt.request-extraction: missing 'verb-path'");
                return -1;
            }

            if (filter_json)
                g_handler.from_mqtt->request_extractor->filter =
                    json_path_filter_from_json_config(filter_json);
        }
        if (event_extraction) {
            g_handler.from_mqtt->event_extractor = message_extractor_new();
            json_object *filter_json = NULL;

            rp_jsonc_unpack(event_extraction, "{s?s s?s s?o}",                              //
                            "data-path", &g_handler.from_mqtt->event_extractor->data_path,  //
                            "event-name-path",
                            &g_handler.from_mqtt->event_extractor->verb_path,  //
                            "filter", &filter_json                             //
            );

            if (filter_json)
                g_handler.from_mqtt->event_extractor->filter =
                    json_path_filter_from_json_config(filter_json);
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

#if 0
static void internal_mosquitto_loop(struct ev_fd *efd, int fd, uint32_t revents, void *closure)
{
    struct mosquitto *mosq = closure;

    mosquitto_loop_read(mosq, /* UNUSED */ 1);
    if (mosquitto_want_write(mosq)) {
        mosquitto_loop_write(mosq, /* UNUSED */ 1);
    }
    mosquitto_loop_misc(mosq);
}
#endif

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

#if 0
    // FIXME: does not always work. Sometimes internal_mosquitto_loop is never called

    // Instead of mosquitto_loop_start() that would start a new thread,
    // we insert the mosquitto loop in the main AFB event loop

    mosquitto_threaded_set(mosq, true);
    int mosquitto_socket_fd = mosquitto_socket(mosq);

    // struct ev_fd *efd = NULL;
    afb_ev_mgr_add_fd(&g_handler.efd, mosquitto_socket_fd, EPOLLIN | EPOLLOUT,
                      internal_mosquitto_loop,
                      /* closure = */ mosq, /* autounref = */ 1, /* autoclose = */ 1);
#else
    mosquitto_loop_start(mosq);
#endif

    // Call event registration verbs, if needed
    if (g_handler.to_mqtt && g_handler.to_mqtt->event_registrations) {
        init_event_registrations();
    }

    LIBAFB_NOTICE("Extension %s ready to serve", AfbExtensionManifest.name);
    return 0;
}

int AfbExtensionExitV1(void *data, struct afb_apiset *declare_set)
{
    LIBAFB_NOTICE("Extension %s got to exit", AfbExtensionManifest.name);

#if 0
    afb_ev_mgr_prepare_wait_dispatch_release(1000);
#endif
    afb_evt_listener_unref(g_listener);

    mqtt_ext_handler_destroy(&g_handler);

    return 0;
}
