
#include "types.h"

/* Route query to a primary/replica and shard.
 *
 * Implementing this function is optional. If the plugin
 * implements it, the query router will use its decision
 * to route the query.
 *
*/
Route pgdog_route_query(Query query);

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
