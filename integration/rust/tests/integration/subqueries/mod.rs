// TODO: shared test skeleton that runs each statement against pgdog **AND** Postgres
// to ensure they return same output...
//
// Will load all **SQL** files in tests dir and do the following:
//
//    - (1) run each over simple and extended protocol
//    - (2) support bind parameters in SQL file
//    - (3) give context in nearby comment to assert...
//          - (1) correct routing,
//          - (2) expected error,
//          - (3) ordered vs unordered,
//          - ...
//    - (4) (reset tables btwn. statements that modify data) OR (have separate tables for each for parallelization)
//    - (5) ensure exact wire protocol conformity w/ Postgres to avoid Postgrex/etc errors
//
// The goal with this is to
//   - reduce mistakes in test coverage,
//   - minimize the code needed to write proper tests,
//   - enhance readability of tests
