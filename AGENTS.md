# AGENTS.md

Repo-specific instructions for coding agents working on this project.

## Repo layout
- `pinot/`: core client library (HTTP + gRPC).
- `gormpinot/`: GORM dialector for Pinot.
- `examples/`: runnable examples (not part of default tests).
- `integration-tests/`: integration tests that require a running Pinot quickstart.
- `scripts/`: helper scripts to start/stop Pinot quickstart.

## Tooling & style
- Go version is defined in `go.mod`; use that version for builds/tests.
- Run `gofmt` on modified Go files and keep `go vet` clean.
- Update `go.mod`/`go.sum` only when dependencies change (run `go mod tidy` if needed).

## Commit requirements
- Enable repo hooks with `make hooks` (uses `.githooks/pre-commit`).
- Pre-commit runs `make lint`, `make test`, and `scripts/check-coverage.sh`.
- Update the coverage baseline with `make coverage-baseline` after legitimate coverage improvements.

## Common commands (from `Makefile`)
- Build: `make build` (or `go build ./...`).
- Unit tests: `make test` (excludes `examples/` and `integration-tests/`).
- Lint: `make lint` (gofmt, go vet, golangci-lint v2).
- Integration tests: `make integration-test` (requires Pinot running).
- Start Pinot quickstart (local dist): `make run-pinot-dist`.
- Stop Pinot quickstart: `make stop-pinot-dist`.
- Start Pinot quickstart (Docker): `make run-pinot-docker`.
- Stop Pinot quickstart (Docker): `make stop-pinot-docker`.

## Integration test notes
- gRPC tests read `BROKER_GRPC_HOST` and `BROKER_GRPC_PORT` (defaults: `127.0.0.1:8010`).
- The quickstart script supports `PINOT_VERSION`, `PINOT_HOME`, `BROKER_PORT_FORWARD`, `BROKER_GRPC_PORT_FORWARD`.
- `scripts/start-pinot-quickstart.sh` uses `curl` and `jq` to verify the cluster is ready.

## When changing public APIs
- Update examples and tests that reference exported types or behavior changes.
