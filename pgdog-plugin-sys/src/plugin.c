/* pgDog plugin C interface. */
#include <stdlib.h>
#include "plugin.h"

Row pgdog_row_new(int num_columns) {
    Row row;

    row.num_columns = num_columns;
    row.columns = (RowColumn *) malloc(num_columns * sizeof(RowColumn));

    return row;
}

void pgdog_row_free(Row row) {
    free(row.columns);
}
