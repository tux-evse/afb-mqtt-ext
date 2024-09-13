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
#include <libafb/core/afb-req-common.h>

#include <rp-utils/rp-jsonc.h>
#include <rp-utils/rp-yaml.h>

#include <mosquitto.h>
#include <uuid/uuid.h>

#include "json-utils.h"

/*************************************************************/
/*************************************************************/
/** AFB-EXTENSION interface                                 **/
/*************************************************************/
/*************************************************************/

AFB_EXTENSION("MQTT")

const struct argp_option AfbExtensionOptionsV1[] = {{.name = "mqtt-config-file",
                                                     .key = 's',
                                                     .arg = "PATH",
                                                     .doc = "Path to a YAML configuration file"},
                                                    {.name = 0, .key = 0, .doc = 0}};

struct message_extractor_t
{
    char *data_path;

    // only useful for requests
    char *verb_path;

    struct json_path_filter_t *filter;
};

void message_extractor_delete(struct message_extractor_t *self)
{
    if (self->filter) {
        json_path_filter_delete(self->filter);
    }
}

// Maximum number of requests waiting for a response
#define REQUEST_QUEUE_LEN 10

struct stored_request_t
{
    json_object *json_request;
    struct afb_req_common *afb_req;
    int timeout_job_id;
};

struct to_mqtt_t
{
    int timeout_ms;
    json_object *request_template;
    // json_object *on_event_template;
    struct message_extractor_t response_extractor;

    struct stored_request_t stored_requests[REQUEST_QUEUE_LEN];
};

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

int to_mqtt_get_request_index(struct to_mqtt_t *self, struct afb_req_common *req)
{
    for (size_t i = 0; i < REQUEST_QUEUE_LEN; i++) {
        if (self->stored_requests[i].afb_req == req) {
            return i;
        }
    }
    return -1;
}

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
                printf("Found !\n");

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

void to_mqtt_delete(struct to_mqtt_t *self)
{
    if (self->request_template)
        json_object_put(self->request_template);
    message_extractor_delete(&self->response_extractor);
}

struct from_mqtt_t
{
    char *api_name;

    int timeout_ms;
    struct message_extractor_t request_extractor;
    json_object *response_template;
    struct message_extractor_t event_extractor;
};

void from_mqtt_delete(struct from_mqtt_t *self)
{
    if (self->response_template)
        json_object_put(self->response_template);
    message_extractor_delete(&self->request_extractor);
    message_extractor_delete(&self->event_extractor);    
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
};

void mqtt_ext_handler_delete(struct mqtt_ext_handler_t *self)
{
    if (self->mosq)
        mosquitto_destroy(self->mosq);

    to_mqtt_delete(&self->to_mqtt);
    from_mqtt_delete(&self->from_mqtt);

    json_object_put(self->config_json);

    afb_apiset_unref(self->call_set);

    free(self);
}

struct mqtt_ext_handler_t *g_handler = NULL;

char default_mqtt_broker_host[] = "localhost";
const int default_mqtt_broker_port = 1883;
const int default_mqtt_timeout_ms = 60000;

struct mqtt_ext_handler_t *mqtt_ext_handler_new()
{
    struct mqtt_ext_handler_t *handler = calloc(1, sizeof(struct mqtt_ext_handler_t));

    handler->broker_host = default_mqtt_broker_host;
    handler->broker_port = default_mqtt_broker_port;

    handler->to_mqtt.timeout_ms = default_mqtt_timeout_ms;

    return handler;
}

int AfbExtensionConfigV1(void **data, struct json_object *config, const char *uid)
{
    LIBAFB_NOTICE("Extension %s got config %s", AfbExtensionManifest.name,
                  json_object_get_string(config));

    g_handler = mqtt_ext_handler_new();

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
        g_handler->config_json = config_file_json;

        char *mapping_type = NULL;
        json_object *to_mqtt_json = NULL, *from_mqtt_json = NULL;

        // clang-format off
        rp_jsonc_unpack(config_file_json, "{s?s s?i s?s s?s s?s s?o s?o}",
            "broker-host", &g_handler->broker_host,
            "broker-port", &g_handler->broker_port,
            "mapping-type", &mapping_type,
            "subscribe-topic", &g_handler->subscribe_topic,
            "publish-topic", &g_handler->publish_topic,
            "to-mqtt", &to_mqtt_json,
            "from-mqtt", &from_mqtt_json
        );
        // clang-format on

        if (strcmp(mapping_type, "topic-pair")) {
            LIBAFB_ERROR("[%s] Unsupported mapping type", AfbExtensionManifest.name);
            return -1;
        }

        if (to_mqtt_json) {
            json_object *extraction = NULL;

            // clang-format off
            rp_jsonc_unpack(to_mqtt_json, "{s?i s?O s?o}",
                "timeout-ms", &g_handler->to_mqtt.timeout_ms,
                "request-template", &g_handler->to_mqtt.request_template,
                "response-extraction", &extraction
            );
            // clang-format on

            if (extraction) {
                json_object *filter_json = NULL;

                // clang-format off
                rp_jsonc_unpack(extraction, "{s?s s?o}",
                    "data-path", &g_handler->to_mqtt.response_extractor.data_path,
                    "filter", &filter_json
                );
                // clang-format on

                if (filter_json)
                    g_handler->to_mqtt.response_extractor.filter =
                        json_path_filter_from_json_config(filter_json);
            }
        }

        if (from_mqtt_json) {
            json_object *extraction = NULL, *event_extraction = NULL;

            // clang-format off
            rp_jsonc_unpack(from_mqtt_json, "{s?i s?s s?O s?o s?o}",
                "timeout-ms", &g_handler->from_mqtt.timeout_ms,
                "api", &g_handler->from_mqtt.api_name,
                "response-template", &g_handler->from_mqtt.response_template,
                "request-extraction", &extraction,
                "event-extraction", &event_extraction
            );
            // clang-format on

            if (extraction) {
                json_object *filter_json = NULL;

                // clang-format off
                rp_jsonc_unpack(extraction, "{s?s s?s s?o}",
                    "data-path", &g_handler->from_mqtt.request_extractor.data_path,
                    "verb-path", &g_handler->from_mqtt.request_extractor.verb_path,                    
                    "filter", &filter_json
                );
                // clang-format on

                if (filter_json)
                    g_handler->from_mqtt.request_extractor.filter =
                        json_path_filter_from_json_config(filter_json);
            }

            if (event_extraction) {
                json_object *filter_json = NULL;

                // clang-format off
                rp_jsonc_unpack(event_extraction, "{s?s s?s s?o}",
                    "data-path", &g_handler->from_mqtt.event_extractor.data_path,
                    "verb-path", &g_handler->from_mqtt.event_extractor.verb_path,                    
                    "filter", &filter_json
                );
                // clang-format on

                if (filter_json)
                    g_handler->from_mqtt.event_extractor.filter =
                        json_path_filter_from_json_config(filter_json);
            }
        }        

    }
    if (data) {
        *data = g_handler;
    }
    return 0;
}

void on_response_timeout(int signal, void *data)
{
    if (signal)
        return;

    printf("Response timeout signal %d\n", signal);
    struct afb_req_common *req = (struct afb_req_common *)data;
    int index = to_mqtt_get_request_index(&g_handler->to_mqtt, req);

    if (index == -1) {
        // No request found, it has probably already been responsed to.
        // Do nothing then
        return;
    }

    printf("Found req\n");
    json_object_put(g_handler->to_mqtt.stored_requests[index].json_request);
    g_handler->to_mqtt.stored_requests[index].afb_req = NULL;

    struct afb_data *reply;
    afb_data_create_raw(&reply, &afb_type_predefined_stringz, "Timeout waiting for response", 0,
                        NULL, NULL);
    afb_req_common_reply(req, AFB_ERRNO_TIMEOUT, 1, &reply);
    afb_req_common_unref(req);
}

// static struct afb_api_itf to_mqtt_api_itf;

struct my_req_t
{
    struct afb_req_common req;

    json_object *request_json;
};

void on_verb_call_reply(struct afb_req_common *req,
                        int status,
                        unsigned nreplies,
                        struct afb_data *const replies[])
{
    struct my_req_t *my_req = containerof(struct my_req_t, req, req);

    printf("**REPLIED status: %d nreplies: %d\n", status, nreplies);

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
        json_object_fill_template(g_handler->from_mqtt.response_template, mapping);
    const char *filled_str = json_object_get_string(filled);

    mosquitto_publish(g_handler->mosq, /* mid = */ NULL, g_handler->publish_topic,
                      strlen(filled_str), filled_str,
                      /* qos = */ 0, /* retain = */ false);

    json_object_put(filled);
}

void on_my_req_unref(struct afb_req_common *req)
{
    printf("ON MY REQ UNREF\n");
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

void on_req_unref(struct afb_req_common *req)
{
    printf("ON REQ UNREF\n");
    free(req);
}

struct afb_req_common_query_itf verb_call_itf = {.reply = on_verb_call_reply,
                                                 .unref = on_my_req_unref};
struct afb_req_common_query_itf verb_call_no_reply_itf = {.reply = on_verb_call_no_reply,
                                                          .unref = on_req_unref};

void on_mqtt_message(struct mosquitto *mosq, void *user_data, const struct mosquitto_message *msg)
{
    printf("** MESSAGE RECEIVED topic: %s ", msg->topic);
    printf("payload: <%.*s>\n", msg->payloadlen, (char *)msg->payload);

    struct mqtt_ext_handler_t *handler = (struct mqtt_ext_handler_t *)user_data;

    // Parse the received message as JSON
    json_tokener *tokener = json_tokener_new();
    json_object *mqtt_json = json_tokener_parse_ex(tokener, msg->payload, msg->payloadlen);
    json_tokener_free(tokener);

    if (!mqtt_json) {
        // Not a valid JSON, abort
        printf("Not a valid JSON\n");
        return;
    }

    int request_idx;

    if (from_mqtt_is_request(&handler->from_mqtt, mqtt_json)) {
        json_object *verb =
            json_object_get_path(mqtt_json, handler->from_mqtt.request_extractor.verb_path);
        const char *verb_str = verb ? json_object_get_string(verb) : NULL;
        json_object *data =
            json_object_get_path(mqtt_json, handler->from_mqtt.request_extractor.data_path);

        struct my_req_t *my_req = malloc(sizeof(struct my_req_t));
        my_req->request_json = json_object_get(mqtt_json);

        struct afb_data *reply[2];
        char *call_type = "request";
        afb_data_create_copy(&reply[0], &afb_type_predefined_stringz, call_type,
                             strlen(call_type) + 1);
        afb_data_create_raw(&reply[1], &afb_type_predefined_json_c, json_object_get(data), 0,
                            (void *)json_object_put, data);
        afb_req_common_init(&my_req->req, /* afb_req_common_query_itf = */ &verb_call_itf,
                            g_handler->from_mqtt.api_name, verb_str, 2, reply, NULL);
        afb_req_common_process(&my_req->req, handler->call_set);
    }
    else if (from_mqtt_is_event(&handler->from_mqtt, mqtt_json)) {
        json_object *verb =
            json_object_get_path(mqtt_json, handler->from_mqtt.event_extractor.verb_path);
        const char *verb_str = verb ? json_object_get_string(verb) : NULL;
        json_object *data =
            json_object_get_path(mqtt_json, handler->from_mqtt.event_extractor.data_path);

        struct afb_req_common *req = malloc(sizeof(struct afb_req_common));
        struct afb_data *reply[2];
        char *call_type = "event";
        afb_data_create_copy(&reply[0], &afb_type_predefined_stringz, call_type,
                             strlen(call_type) + 1);
        afb_data_create_raw(&reply[1], &afb_type_predefined_json_c, json_object_get(data), 0,
                            (void *)json_object_put, data);
        afb_req_common_init(req, /* afb_req_common_query_itf = */ &verb_call_no_reply_itf,
                            g_handler->from_mqtt.api_name, verb_str, 2, reply, NULL);
        afb_req_common_process(req, handler->call_set);
    }
    else if ((request_idx = to_mqtt_match_reponse(&handler->to_mqtt, mqtt_json)) != -1) {
        struct stored_request_t *stored_request = &handler->to_mqtt.stored_requests[request_idx];

        // disarm the response timeout
        afb_sched_abort_job(stored_request->timeout_job_id);

        // extract useful data from response
        json_object *response_json = mqtt_json;
        if (handler->to_mqtt.response_extractor.data_path) {
            response_json =
                json_object_get_path(mqtt_json, handler->to_mqtt.response_extractor.data_path);
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
        to_mqtt_delete_stored_request(&handler->to_mqtt, request_idx);
    }
    json_object_put(mqtt_json);
}

json_object *make_uuid()
{
    uuid_t uuid;
    uuid_generate((unsigned char *)&uuid);
    char uuid_str[37];
    snprintf(uuid_str, 37, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             uuid[0], uuid[1], uuid[2], uuid[3], uuid[4], uuid[5], uuid[6], uuid[7], uuid[8],
             uuid[9], uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15]);
    return json_object_new_string(uuid_str);
}

struct template_function_t id_functions[] = {{.function_name = "uuid", .generator = make_uuid},
                                             {.function_name = NULL}};

static void on_to_mqtt_request(void *closure, struct afb_req_common *req)
{
    struct mqtt_ext_handler_t *handler = (struct mqtt_ext_handler_t *)g_handler;

    printf("to_mqtt_process api:%s verb:%s\n", req->apiname, req->verbname);

    if (!handler->to_mqtt.request_template) {
        // error
    }

    struct afb_data *arg_json = NULL;
    int rc = afb_req_common_param_convert(req, 0, &afb_type_predefined_json_c, &arg_json);
    if (rc < 0) {
        LIBAFB_NOTICE("Cannot convert argument to JSON");
        return;
    }
    json_object *arg = afb_data_ro_pointer(arg_json);

    if (handler->publish_topic) {
        json_object *mapping = json_object_new_object();
        json_object_object_add(mapping, "verb", json_object_new_string(req->verbname));
        json_object_object_add(mapping, "data", arg);

        json_object *filled = json_object_fill_template_with_functions(
            handler->to_mqtt.request_template, mapping, id_functions);
        const char *request_str = json_object_get_string(filled);

        mosquitto_publish(handler->mosq, /* mid = */ NULL, handler->publish_topic,
                          strlen(request_str), request_str,
                          /* qos = */ 0, /* retain = */ false);

        req = afb_req_common_addref(req);
        // start a timeout job
        int job_id =
            afb_sched_post_job(/* group = */ NULL, /* delayms = */
                               g_handler->to_mqtt.timeout_ms,
                               /* timeout = */ 0, on_response_timeout, req, Afb_Sched_Mode_Normal);
        printf("job_id %d\n", job_id);

        to_mqtt_add_stored_request(&handler->to_mqtt, req, filled, job_id);
    }
    printf("*******\n");
}

static struct afb_api_itf to_mqtt_api_itf = {.process = on_to_mqtt_request};

int AfbExtensionDeclareV1(void *data, struct afb_apiset *declare_set, struct afb_apiset *call_set)
{
    struct mqtt_ext_handler_t *handler = (struct mqtt_ext_handler_t *)data;
    LIBAFB_NOTICE("Extension %s successfully registered", AfbExtensionManifest.name);

    struct afb_api_item to_mqtt_api_item;
    to_mqtt_api_item.itf = &to_mqtt_api_itf;
    to_mqtt_api_item.group = (const void *)1;
    to_mqtt_api_item.closure = data;

    struct afb_apiset *ds = declare_set;
    ds = afb_apiset_subset_find(ds, "public");

    int rc;
    if ((rc = afb_apiset_add(ds, "to_mqtt", to_mqtt_api_item)) < 0) {
        LIBAFB_ERROR("Error calling afb_apiset_add (to_mqtt): %d\n", rc);
    }

    // Register the call set so that we are able to issue verb calls
    handler->call_set = afb_apiset_addref(call_set);
    return 0;
}

int AfbExtensionHTTPV1(void *data, struct afb_hsrv *hsrv)
{
    LIBAFB_NOTICE("Extension %s got HTTP", AfbExtensionManifest.name);
    return 0;
}

int AfbExtensionServeV1(void *data, struct afb_apiset *call_set)
{
    if (mosquitto_lib_init() != MOSQ_ERR_SUCCESS) {
        LIBAFB_ERROR("Error calling mosquitto_lib_init");
        return -1;
    }

    struct mqtt_ext_handler_t *handler = (struct mqtt_ext_handler_t *)data;

    struct mosquitto *mosq =
        mosquitto_new("afb_mqtt_client", /* clean_session = */ true, /* void *obj = */ handler);
    if (mosq == NULL) {
        LIBAFB_ERROR("Error calling afb_mqtt_client\n");
        return -1;
    }

    handler->mosq = mosq;

    int rc = mosquitto_connect(mosq, handler->broker_host, handler->broker_port,
                               /* keepalive = */ 5);
    if (rc != MOSQ_ERR_SUCCESS) {
        LIBAFB_ERROR("Error on connect: %s", mosquitto_strerror(rc));
        return -1;
    }

    if (handler->subscribe_topic) {
        rc = mosquitto_subscribe(mosq, NULL, handler->subscribe_topic, /* qos = */ 0);
        if (rc != MOSQ_ERR_SUCCESS) {
            LIBAFB_ERROR("Cannot connect to %s: %s", handler->subscribe_topic,
                         mosquitto_strerror(rc));
            return -1;
        }
    }

    mosquitto_message_callback_set(mosq, on_mqtt_message);

    // Start a thread to handle requests
    mosquitto_loop_start(mosq);

    LIBAFB_NOTICE("Extension %s ready to serve", AfbExtensionManifest.name);
    return 0;
}

int AfbExtensionExitV1(void *data, struct afb_apiset *declare_set)
{
    struct mqtt_ext_handler_t *handler = (struct mqtt_ext_handler_t *)data;
    LIBAFB_NOTICE("Extension %s got to exit", AfbExtensionManifest.name);
    mosquitto_loop_stop(handler->mosq, /* force = */ true);

    mqtt_ext_handler_delete(data);
    return 0;
}
