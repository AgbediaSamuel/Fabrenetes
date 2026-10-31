# Fabrenetes

Fabrenetes runs applications across multiple machines and lets you see where they are running. It can also start an application again when it crashes. It was inspired by Azure Service Fabric and Kubernetes.

This is an ongoing project for Minerva University's Distributed Systems Tutorial, Fall 2026.

## How it works

A service describes an application command and how many copies to run. The coordinator chooses workers for those copies. Worker agents start the applications on their machines and report their status.

New copies go to the available worker with the fewest assigned copies. Workers can allocate separate ports and restart crashed applications while communicating with the coordinator.

## Try it locally

You need Go 1.24 or newer and Node.js. First, install the command from the repository root:

```sh
GOBIN="$HOME/.local/bin" go install ./cmd/fabrenetes
```

Add this line to your shell's startup file, such as `~/.zshrc` for zsh, then open new terminals:

```sh
export PATH="$HOME/.local/bin:$PATH"
```

`PATH` tells your shell where to find commands. You can now call `fabrenetes` from any directory.

This example runs one worker on your own machine. Open three terminals. Keep the worker terminal in the repository root so it can find `./demo/clock`; the other two can be anywhere.

The coordinator listens at `http://127.0.0.1:8080` by default. The worker and terminal commands connect there automatically, so this example needs no extra configuration.

Terminal 1 — coordinator:

```sh
fabrenetes coordinator
```

Terminal 2 — worker:

```sh
fabrenetes worker --id local
```

Terminal 3 — create and inspect an application:

```sh
fabrenetes create --name clock --port 9001 ./demo/clock
fabrenetes services
```

Application paths are resolved on the worker, from the directory where it started. An absolute application path also works; installing the CLI does not install the clock application on other machines.

Once the clock shows `running`, request its time endpoint. If port 9001 was occupied, use the port shown by `services` instead.

```sh
curl http://127.0.0.1:9001/time
```

Open the [dashboard](http://127.0.0.1:8080/) and click **Stop instance**. The worker starts a replacement: its process ID changes and the restart count increases.

When finished, remove the application, then use Ctrl-C in the worker and coordinator terminals:

```sh
fabrenetes delete clock
```

The coordinator address can be a different port on the same computer or an address on another machine. For example, to use port 8081 locally, start the coordinator with:

```sh
fabrenetes coordinator --listen 127.0.0.1:8081
```

Then set the optional `coordinator_url` in the worker and CLI terminals so they connect to that address:

```sh
export coordinator_url=http://127.0.0.1:8081
```

You can also pass `--coordinator <url>` to a single command. `--listen` chooses where the coordinator listens; `coordinator_url` chooses where workers and CLI commands connect.

## CLI and API

The `coordinator` and `worker` commands run those programs. Other commands call the coordinator's HTTP API:

| CLI command | API request | What it does |
| --- | --- | --- |
| `fabrenetes workers` | `GET /workers` | Lists workers, their IP addresses, and status. |
| `fabrenetes create` | `POST /services` | Creates a service from a command and a number of copies. |
| `fabrenetes services` | `GET /services` | Shows where each copy runs, its port, process ID, status, and restart count. |
| `fabrenetes delete <name>` | `DELETE /services/{name}` | Removes a service and tells its workers to stop its applications. |

Workers register at `POST /workers` and send updates to `POST /workers/{id}/heartbeat`. The reply tells them what to run. The dashboard shares this API; see [server.go](internal/coordinator/server.go).

## Where to look

The dashboard shows applications by worker and lets you create or delete services. **Start worker** and **Stop worker** also control agents in configured Linux VMs while leaving the VMs running.

The [clock application](demo/clock) can be used to demo Fabrenetes. Its `GET /time` response includes UTC time, the worker and service, and the instance and process IDs. Use the worker's IP and displayed port to check which copy responds.

To follow the code, start here:

1. [cmd/fabrenetes/main.go](cmd/fabrenetes/main.go) handles commands; [services.go](cmd/fabrenetes/services.go) sends service requests.
2. [internal/api/api.go](internal/api/api.go) defines the messages they send.
3. [internal/coordinator/server.go](internal/coordinator/server.go) receives requests; [services.go](internal/coordinator/services.go) chooses where applications run. [vms.go](internal/coordinator/vms.go) handles VM commands.
4. [internal/worker/worker.go](internal/worker/worker.go) receives assignments; [processes.go](internal/worker/processes.go) starts and restarts applications; [ports.go](internal/worker/ports.go) chooses ports.

The other folders are:

- [internal/coordinator/dashboard/](internal/coordinator/dashboard/): the web dashboard.
- [demo/](demo/): a clock application that can be used to demo Fabrenetes.
- [examples/](examples/): another sample application.
- [deploy/](deploy/): the Linux setup file for running a worker in the background.

## Checks

Run these from the repository root:

```sh
go test -race ./...
go vet ./...
```

The tests cover worker registration, application placement, process recovery, port allocation, and VM commands. The race check looks for unsafe access to shared data; `go vet` checks for likely Go mistakes.

## Current limits

Applications run as ordinary processes and must already be installed on the workers. Restarting the coordinator loses its service definitions. If a worker becomes unreachable, its applications are not automatically moved elsewhere. Containers, request balancing, and replicated application data are still future work.
