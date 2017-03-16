A running Postgres instance is required for the maven integration-test
phase of some modules like domain-models-runtime. Starting a new embedded
Postgres instance for each test class takes too much time, instead start
and stop embedded Postgres once in the pre-integration-test and
post-integration-test phase using these helper programs.

PostgresRunner starts a Postgres instance if the port is free and it
does nothing if the port is in use (by some running Postgres). Starting
an embedded Postgres may take a minute or more. In all cases
PostgresRunner opens a port for PostgresWaiter and PostgresStopper.
PostgresRunner runs in the background (spawn).

PostgresWaiter waits until Postgres is ready; when PostgresWaiter returns
Postgres is ready and integration tests may start.

PostgresStopper sends a signal to PostgresRunner to stop both
Postgres and PostgresRunner.

