/*
 * Copyright (C) 2015-2024 IoT.bzh Company
 * Author: Hugo Mercier <hugo.mercier@iot.bzh>
 *
 */
#include "json-utils.h"

#include <libafb/afb-misc.h>  // LIBAFB_NOTICE

#include <stdbool.h>
#include <string.h>

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

json_object *json_object_fill_template(json_object *jso, json_object *mapping)
{
    switch (json_object_get_type(jso)) {
    case json_type_array: {
        json_object *output = json_object_new_array();
        size_t len = json_object_array_length(jso);
        for (size_t i = 0; i < len; i++) {
            json_object_array_put_idx(
                output, i, json_object_fill_template(json_object_array_get_idx(jso, i), mapping));
        }
        return output;
    }
    case json_type_object: {
        json_object *output = json_object_new_object();
        json_object_object_foreach(jso, key, value)
        {
            json_object_object_add(output, key, json_object_fill_template(value, mapping));
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