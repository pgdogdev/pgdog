
#include "types.h"

/*
 * Perform initialization at plugin loading time.
 *
 * Executed only once and execution is synchronized,
 * so it's safe to initialize sychroniziation primitives
 * like mutexes in this method.
 */
void pgdog_init();

/*
 * Shutdown procedure for plugin.
 */
void pgdog_fini();
