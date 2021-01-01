Environment Variables
=====================

The following are environment variables that can affect the behavior of the
system.
- `AWS_DEFAULT_REGION`: If set and no region has been otherwise configured
  (e.g., via $HOME/.aws/config), use this as the AWS region for accessing
  services. Otherwise, the region used will be `us-east-1`. This mirrors the
  configuration behavior of the AWS S3 CLI client.
- `KATANA_AWS_TEST_ENDPOINT`: If set, use this as the endpoint to access S3
  rather than the standard AWS endpoint(s). This can be useful for testing.
- `KATANA_DO_NOT_BIND_THREADS`: By default, the thread runtime will bind the worker
  threads to specific cores. Setting this value, `KATANA_DO_NOT_BIND_THREADS=1`, will
  disable this behavior.
- `KATANA_BIND_MAIN_THREAD`: By default, the thread runtime will not bind the
  main thread to a specific core. Setting this value,
  `KATANA_BIND_MAIN_THREAD=1`, will bind a thread to a specific core. This can
  be useful when optimizing performance for certain workloads though it comes
  at the expense of inhibiting composition of applications linked with the
  Galois library with other threading libraries.
- `KATANA_LOG_LEVEL`: Set the minimum level of log message to output.
  The log levels are 0 (Debug), 1 (Verbose), 2 (Info), 3 (Warning), 4 (Error).
  By default, print everything (level 0). The presence of debug messages also requires
  a debug build. The default log level is 0.

  Presently, there is a second, legacy, logging system which is controlled by a
  separate series of environment variables: `KATANA_DEBUG_TRACE_STDERR`,
  `KATANA_DEBUG_SKIP`, `KATANA_DEBUG_TO_FILE`, `KATANA_DEBUG_TRACE`.
