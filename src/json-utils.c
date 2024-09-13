/*
 * Copyright (C) 2015-2024 IoT.bzh Company
 * Author: Hugo Mercier <hugo.mercier@iot.bzh>
 *
 */
#include "json-utils.h"

#include <libafb/afb-misc.h>  // LIBAFB_NOTICE

#include <stdbool.h>
#include <string.h>

#include <stdio.h>

json_object *json_object_get_path(json_object *obj, const char *path)
{
    size_t offset = 0;

    while (true) {
        if (!path[offset] || path[offset] != '.') {
            LIBAFB_NOTICE("Wrong path format %s", path);
            return NULL;
        }
        size_t end = offset + 1;
        while (path[end] && path[end] != '.')
            end++;

        char path_part[end - offset + 1];
        strncpy(path_part, path + offset + 1, end - offset);
        path_part[end - offset] = 0;

        json_object *json_child = NULL;
        if (json_object_object_get_ex(obj, path_part, &json_child)) {
            if (path[end]) {
                obj = json_child;
                offset = end;
                continue;
            }
            else {
                return json_child;
            }
        }
        return NULL;
    }
}

struct template_function_t *template_functions_find(struct template_function_t *functions,
                                                    char *function_name)
{
    struct template_function_t *p = functions;
    while (p->function_name && strcmp(p->function_name, function_name))
        p++;
    return p->function_name ? p : NULL;
}

json_object *json_object_fill_template_with_functions(json_object *jso,
                                                      json_object *mapping,
                                                      struct template_function_t *functions)
{
    switch (json_object_get_type(jso)) {
    case json_type_array: {
        json_object *output = json_object_new_array();
        size_t len = json_object_array_length(jso);
        for (size_t i = 0; i < len; i++) {
            json_object_array_put_idx(output, i,
                                      json_object_fill_template_with_functions(
                                          json_object_array_get_idx(jso, i), mapping, functions));
        }
        return output;
    }
    case json_type_object: {
        json_object *output = json_object_new_object();
        json_object_object_foreach(jso, key, value)
        {
            json_object_object_add(
                output, key, json_object_fill_template_with_functions(value, mapping, functions));
        }
        return output;
    }
    case json_type_string: {
        const char *str = json_object_get_string(jso);
        size_t len = strlen(str);
        if ((len >= 3) && (str[0] == '$') && (str[1] == '{') && (str[len - 1] == '}')) {
            char tag[len - 3 + 1];
            strncpy(tag, str + 2, len - 3);
            tag[len - 3] = 0;
            json_object *replace = NULL;

            char *dot = strchr(tag, '.');
            if (dot) {
                // e.g. request.id
                char tag2[dot - tag + 1];
                strncpy(tag2, tag, dot - tag);
                if (json_object_object_get_ex(mapping, tag2, &replace)) {
                    return json_object_get(json_object_get_path(replace, dot));
                }
            }

            char *function_call = strstr(tag, "()");
            if (function_call) {
                char function_name[function_call - tag + 1];
                strncpy(function_name, tag, function_call - tag);
                function_name[function_call - tag] = 0;
                struct template_function_t *function =
                    template_functions_find(functions, function_name);
                if (function) {
                    return function->generator();
                }
            }
            if (json_object_object_get_ex(mapping, tag, &replace)) {
                // increment the ref of replace
                return json_object_get(replace);
            }
        }
    }
    default:
        // just copy by incrementing the ref
        return json_object_get(jso);
    }
}

json_object *json_object_fill_template(json_object *jso, json_object *mapping)
{
    return json_object_fill_template_with_functions(jso, mapping, NULL);
}

struct json_path_filter_t
{
    char *path;
    json_object *expected_value;
};

struct json_path_filter_t *json_path_filter_new(char *path, json_object *expected_value)
{
    struct json_path_filter_t *filter = calloc(1, sizeof(struct json_path_filter_t));
    filter->path = strdup(path);
    filter->expected_value = json_object_get(expected_value);
    return filter;
}

void json_path_filter_delete(struct json_path_filter_t *self)
{
    if (self->path)
        free(self->path);
    if (self->expected_value)
        json_object_put(self->expected_value);
}

bool json_path_filter_does_apply(struct json_path_filter_t *self, json_object *obj)
{
    json_object *sub = json_object_get_path(obj, self->path);
    if (!sub)
        return false;

    // for now only string comparison are supported
    const char *value_str = json_object_get_string(sub);
    const char *expected_value_str = json_object_get_string(self->expected_value);
    return !strcmp(value_str, expected_value_str);
}

struct json_path_filter_t *json_path_filter_from_json_config(json_object *json_config)
{
    char *path = NULL;
    json_object *value = NULL, *json_item = NULL;
    if (json_object_object_get_ex(json_config, "path", &json_item)) {
        path = (char *)json_object_get_string(json_item);
    }
    if (json_object_object_get_ex(json_config, "value", &json_item)) {
        value = json_item;
    }
    return json_path_filter_new(path, value);
}