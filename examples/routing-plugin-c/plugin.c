
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include "../../pgdog-plugin/src/plugin.h"

Route route(Query query) {
    Route route;
    char *lowercase = strdup(query.query);

    for (int i = 0; i < strlen(lowercase); i++) {
        lowercase[i] = tolower(lowercase[i]);
    }

    if (!strncmp(lowercase, "select", strlen("select"))) {
        route.affinity = READ;
    } else {
        route.affinity = WRITE;
    }

    free(lowercase);

    route.shard = -1;

    return route;
}
