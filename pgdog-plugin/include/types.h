/*
 * Query.
*/
typedef struct Value {
    int len;
    const char *data;
    int oid;
} Value;

typedef struct Query {
    int len;
    const char *query;
    int num_values;
    const Value *values;
} Query;

typedef enum Affinity {
    READ = 1,
    WRITE = 2,
    UNKNOWN = 3,
} Affinity;

typedef enum Shard {
    ANY = -1,
    ALL = -2,
} Shard;

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
