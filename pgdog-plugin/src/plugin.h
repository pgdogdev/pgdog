
typedef struct Query {
    int len;
    char *query;
} Query;


typedef enum Affinity {
    READ = 1,
    WRITE = 2,
} Affinity;

typedef struct Route {
    Affinity affinity;
    int shard;
} Route;
