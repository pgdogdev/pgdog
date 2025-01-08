
/*
 * Query parameter value.
 */
typedef struct Parameter {
    int len;
    const char *data;
    int format;
} Parameter;

/*
 * Query and parameters received by pgDog.
 *
 * The plugin is expected to parse the query and based on its
 * contents and the parameters, make a routing decision.
*/
typedef struct Query {
    int len;
    const char *query;
    int num_parameters;
    const Parameter *parameters;
} Query;

/*
 * The query is a read or a write.
 * In case the plugin isn't able to figure it out, it can return UNKNOWN and
 * pgDog will ignore the plugin's decision.
*/
typedef enum Affinity {
    READ = 1,
    WRITE = 2,
    TRANSACTION_START = 3,
    TRANSACTION_END = 4,
    UNKNOWN = -1,
} Affinity;

/*
 * In case the plugin doesn't know which shard to route the
 * the query, it can decide to route it to any shard or to all
 * shards. All shard queries return a result assembled by pgDog.
 *
*/
typedef enum Shard {
    ANY = -1,
    ALL = -2,
} Shard;

/*
 * Route the query should take.
 *
*/
typedef struct Route {
    Affinity affinity;
    int shard;
} Route;

/*
 * The routing decision the plugin makes based on the query contents.
 *
 * FORWARD: The query is forwarded to a shard. Which shard (and whether it's a replica
 *           or a primary) is decided by the plugin output.
 * REWRITE: The query text is rewritten. The plugin outputs new query text.
 * ERROR: The query is denied and the plugin returns an error instead. This error is sent
 *        to the client.
 * INTERCEPT: The query is intercepted and the plugin returns rows instead. These rows
              are sent to the client and the original query is never sent to a backend server.
 * NO_DECISION: The plugin doesn't care about this query. The output is ignored by pgDog and the next
                plugin in the chain is attempted.
 *
*/
typedef enum RoutingDecision {
    FORWARD = 1,
    REWRITE = 2,
    ERROR = 3,
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

/*
 * Union of results a plugin can return.
 *
 * Route: FORWARD
 * Error: ERROR
 * Intercept: INTERCEPT
 *
 */
typedef union RoutingOutput {
    Route route;
    Error error;
    Intercept intercept;
} RoutingOutput;

/*
 * Plugin output.
 *
 * This is returned by a plugin to communicate its routing decision.
 */
typedef struct Output {
    RoutingDecision decision;
    RoutingOutput output;
} Output;

/*
 * Database role, e.g. primary or replica.
*/
typedef enum Role {
    PRIMARY = 1,
    REPLICA = 2,
} Role;

/*
 * Database configuration entry.
*/
typedef struct DatabaseConfig {
    int shard;
    Role role;
    char *host;
    int port;
} DatabaseConfig;

/*
 * Configuration for a database cluster
 * used to the serve a query passed to the plugin.
*/
typedef struct Config {
    int num_databases;
    DatabaseConfig *databases;
    /* Database name from pgdog.toml. */
    char *name;
} Config;

/*
* Routing input union passed to the plugin.
*/
typedef union RoutingInput {
    Query query;
} RoutingInput;

/*
 * Input type.
*/
typedef enum InputType {
    ROUTING_INPUT = 1,
} InputType;

/*
 * Plugin input.
*/
typedef struct Input {
    Config config;
    InputType input_type;
    RoutingInput input;
} Input;
