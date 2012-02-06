/*
 * Copyright (C) 2012 Ron Pedde <ron@pedde.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses
 */

#include <stdio.h>
#include <stdlib.h>

#include <collectd.h>
#include <common.h>
#include <plugin.h>

#include <libcarbon.h>

/**
 * forwards
 */

static int cg_config(const char *key, const char *value);
static int cg_init(void);
static int cg_shutdown(void);
static int cg_write(const data_set_t *ds, const value_list_t *vl,
                    user_data_t *ud);

/**
 * globals
 */
static const char *config_keys[] = {
    "graphite_host",
    "graphite_port",
    "graphite_metric_prefix"
};

static int config_keys_num = STATIC_ARRAY_SIZE(config_keys);
static char *graphite_host = NULL;
static uint16_t graphite_port = 0;
static char *graphite_metric_prefix = "systems";


/**
 * callback function for module registration
 */
void module_register(void) {
    plugin_register_config("graphite", cg_config,
                           config_keys, config_keys_num);
    plugin_register_init ("graphite", cg_init);
    plugin_register_shutdown ("graphite", cg_shutdown);
    plugin_register_write ("graphite", cg_write, NULL);
}

/**
 * config parser
 */
static int cg_config(const char *key, const char *value) {
    INFO("Config for %s: %s", key, value);
    if(strcmp(key,"graphite_host") == 0) {
        graphite_host = strdup(value);
    } else if (strcmp(key,"graphite_port") == 0) {
        graphite_port = strtoul(value, NULL, 10);
    } else if (strcmp(key,"graphite_metric_prefix") == 0) {
        graphite_metric_prefix = strdup(value);
        while(graphite_metric_prefix[strlen(graphite_metric_prefix)-1] == '.')
            graphite_metric_prefix[strlen(graphite_metric_prefix)-1] = '\x0';
    }
    return 0;
}


/**
 * init function
 */
static int cg_init(void) {
    INFO("graphite: Init");
    return libcarbon_init(graphite_host, graphite_port);
}

/**
 * shutdown function
 */
static int cg_shutdown(void) {
    INFO("graphite: shutdown");
    return libcarbon_deinit();
}

/**
 * write function
 */

static int cg_write (const data_set_t *ds, const value_list_t *vl,
                     user_data_t *ud) {
    char buffer[512];
    int buffer_len=512;
    uint64_t value;
    int i;

    if((!graphite_host)||(graphite_port==0)) {
        ERROR("graphite: graphite_host or graphite_port not specified");
        plugin_unregister_write("graphite");
        return(-1);
    }

    for(i = 0; i < ds->ds_num; i++) {
        if((ds->ds[i].type != DS_TYPE_COUNTER)
           && (ds->ds[i].type != DS_TYPE_GAUGE)
           && (ds->ds[i].type != DS_TYPE_DERIVE)
           && (ds->ds[i].type != DS_TYPE_ABSOLUTE))
            return -1;

        memset((void*)&buffer,0,sizeof(buffer));
        ssnprintf(buffer, buffer_len, "%s.%s.%s.%s.%s.%s", graphite_metric_prefix,
                  vl->host, vl->plugin, vl->plugin_instance, vl->type,
                  vl->type_instance);

        switch(ds->ds[i].type) {
        case DS_TYPE_COUNTER:
            value=vl->values[i].counter;
            break;
        case DS_TYPE_GAUGE:
            value=vl->values[i].gauge;
            break;
        case DS_TYPE_DERIVE:
            value=vl->values[i].derive;
            break;
        case DS_TYPE_ABSOLUTE:
            value=vl->values[i].absolute;
            break;
        }

        libcarbon_send(buffer, value, NULL);
    }

    return 0;
}

