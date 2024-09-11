/*
 * Copyright (C) 2015-2024 IoT.bzh Company
 * Author: Hugo Mercier <hugo.mercier@iot.bzh>
 *
 */

#ifndef MQTT_JSON_UTILS
#define MQTT_JSON_UTILS

#include <stdbool.h>

#include <json-c/json.h>

/**
 * Get a part of a JSON object by specifying a (simplified) "JSON-path"
 *
 * This function takes a JSON object and a "path" string. The path is
 * made of object keys separated by a dot (".").
 *
 * e.g ".child1.child2" will return the part of the JSON object that is
 * stored as value of the key "child2" of the object stored as vlaue of
 * the key "child1" of the JSON object.
 *
 * NULL is returned if the path is invalid or not found
 *
 * If the path is correct and a value exists in the supplied JSON
 * object, the corresponding part is returned as a json_object*. No
 * reference count is changed by this function.
 *
 * @param obj  the input json_object on which to extract part of
 * @param path the "JSON-path" to use.
 *
 * @return NULL if the path is invalid or if no value exists for this
 *         path or the part of the JSON object that matches the path
 */
json_object *json_object_get_path(json_object *obj, const char *path);

/**
 * Treat a JSON object as a template where special strings may be
 * replaced with some provided values
 *
 * Any string value in the JSON object that starts with "${" and ends
 * with "}" are treated as a placeholder whose value is replaced.
 *
 * The "mapping" argument is an object that maps strings to any JSON
 * value (including a sub JSON tree).
 *
 * e.g. For an input JSON object like {"data" : "${my_data}"} and a
 * "mapping" object like {"my_data": {"ok": 42}}, the returned JSON
 * object will be {"data": {"ok": 42}}
 *
 * If the name of the placeholder contains a dot ('.'), the part before
 * the dot refers to the key in the mapping and the rest to a JSON
 * "path" inside the associated value.
 * 
 * e.g. considering the mapping of
 * the last example, a template like '{"data": "${my_data.ok}"}' will
 * give '{"data": 42}'
 *
 * @param obj     the input JSON object to parse as a template
 * @param mapping the mapping of values used to fill the template
 *
 * @return a copy of the input JSON object where special strings are
 *         replaced by their values
 */
json_object *json_object_fill_template(json_object *obj, json_object *mapping);

struct json_path_filter_t;

struct json_path_filter_t *json_path_filter_new(char *path,
                                                json_object *expected_value);

bool json_path_filter_does_apply(struct json_path_filter_t *self, json_object *obj);

void json_path_filter_delete(struct json_path_filter_t *self);

#endif