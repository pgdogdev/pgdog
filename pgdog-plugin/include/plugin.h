
#include "types.h"

/* Route query to a primary/replica and shard.
 *
 * Implementing this function is optional. If the plugin
 * implements it, the query router will use its decision
 * to route the query.
 *
 * ## Thread safety
 *
 * This function is not synchronized and can be called
 * for multiple queries at a time. If accessing global state,
 * make sure to protect access with a mutex.
 *
 * ## Performance
 *
 * This function is called for every transaction. It's a hot path,
 * so make sure to optimize for performance in the implementation.
 *
*/
Output pgdog_route_query(Input input);

/*
 * Perform initialization at plugin loading time.
 *
 * Executed only once and execution is synchronized,
 * so it's safe to initialize sychroniziation primitives
 * like mutexes in this method.
 */
void pgdog_init();

/* Create new row.
*
* Implemented by pgdog_plugin library.
* Make sure your plugin links with -lpgdog_plugin.
*/
extern Row pgdog_row_new(int num_columns);

/* Free memory allocated for the row.
*
* Implemented by pgdog_plugin library.
* Make sure your plugin links with -lpgdog_plugin.
*/
extern void pgdog_row_free(Row row);
