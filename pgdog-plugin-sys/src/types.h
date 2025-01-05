/*
 * Query.
*/
typedef struct Query {
    int len;
    const char *query;
} Query;

typedef enum Affinity {
    READ = 1,
    WRITE = 2,
} Affinity;

typedef struct Route {
    Affinity affinity;
    int shard;
} Route;

typedef struct RowColumn {
    int length;
    char *data;
} RowColumn;

typedef struct Row {
    int num_columns;
    RowColumn *columns;
} Row;

typedef struct RowDescriptionColumn {
    int len;
    char *name;
    int oid;
} RowDescriptionColumn;

typedef struct RowDescription {
    int num_columns;
    RowDescriptionColumn *columns;
} RowDescription;
