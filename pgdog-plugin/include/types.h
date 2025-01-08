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

typedef enum RoutingDecision {
    FORWARD = 1,
    REWRITE = 2,
    BLOCK = 3,
    INTERCEPT = 4,
    NO_DECISION = 5, /* The plugin doesn't want to make a decision. We'll try
                 the next plugin in the chain. */
} RoutingDecision;

/*
 * Error returned by the router plugin.
 * This will be sent to the client and the transaction will be aborted.
*/
typedef struct Error {
    char *severity;
    char *code;
    char *message;
    char *detail;
} Error;

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

typedef struct Intercept {
    RowDescription row_description;
    int num_rows;
    Row *rows;
} Intercept;

typedef union RoutingOutput {
    Route route;
    Error error;
    Intercept intercept;
} RoutingOutput;

typedef struct Output {
    RoutingDecision decision;
    RoutingOutput output;
} Output;
