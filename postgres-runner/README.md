A running Postgres instance is required for the maven test phase of domain-models-runtime. Starting a new embedded
Postgres instance for each test class takes too much time, instead start embedded Postgres once before the maven
test phase.

PostgresRunner starts a Postgres instance if the port is free; this may take a minute or more. In addition it opens
a port for PostgresWaiter and PostgresStopper.

PostgresWaiter waits until Postgres is ready.

PostgresStopper sends a signal to PostgresRunner to stop both Postgres and PostgresRunner.
