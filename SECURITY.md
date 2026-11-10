# Security policy

pipelino schedules the functions you give it; it does no I/O and parses no input of its own. A security bug here would
be the scheduler doing something a caller cannot expect, for example running an item after `cancel()` with a signal
that was never aborted.

## Reporting a vulnerability

Please report privately through GitHub: **Security → Report a vulnerability** on
https://github.com/smart-byte/pipelino (a private security advisory). Do not open a public issue for it.

Include the pipelino version, the runtime (browser or Node version), and a small `createPipeline` snippet or the steps
that reproduce the problem.

## Scope

Supported: the latest released version.
