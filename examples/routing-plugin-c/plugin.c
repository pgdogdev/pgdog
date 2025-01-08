
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include "../../pgdog-plugin/include/plugin.h"

void pgdog_init() {
    printf("pgDog routing in C initialized\n");
}

Output pgdog_route_query(Input input) {
    Output plugin_output;
    RoutingOutput routing_output;
    Route route;
    char *lowercase;

    route.shard = ANY; /* No sharding */

    lowercase = strdup(input.input.query.query);

    for (int i = 0; i < strlen(lowercase); i++) {
        lowercase[i] = tolower(lowercase[i]);
    }

    if (!strncmp(lowercase, "select", strlen("select"))) {
        route.affinity = READ;
    } else {
        route.affinity = WRITE;
    }

    free(lowercase);

    routing_output.route = route;
    plugin_output.decision = FORWARD;
    plugin_output.output = routing_output;

    return plugin_output;
}
