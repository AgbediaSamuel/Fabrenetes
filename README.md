# Fabrenetes

Fabrenetes runs applications across multiple machines and lets you see where they are running. It can also start an application again when it crashes. It was inspired by Azure Service Fabric and Kubernetes.

This is an ongoing project for Minerva University's Distributed Systems Tutorial, Fall 2026.

## How it works

A service describes an application command and how many copies to run. The coordinator chooses workers for those copies. Worker agents start the applications on their machines and report their status.

New copies go to the available worker with the fewest assigned copies. Workers can allocate separate ports and restart crashed applications while communicating with the coordinator.

Stateful services also get a dictionary that Fabrenetes stores on three different workers. Applications use it through an HTTP API or the Go client in [collections/](collections/).

Each service has an address under `/apps/<name>/`. The coordinator forwards requests to its running copies in turn, using the worker's IP and the port it reports. Failed requests are returned to the caller rather than sent again to another copy.

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

You can also use `http://127.0.0.1:8080/apps/clock/time`, which keeps the same address as application ports change.

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

## Upload an application

You can upload a ZIP containing the application, its dependencies, and its runtime. For example, a package could contain `runtime/node` and `server.js` at its root:

```sh
fabrenetes create --name clock --package clock.zip --port 9001 --replicas 2 ./runtime/node server.js
```

Workers download the package and run the command from a separate directory for each copy. The command and arguments use paths relative to the ZIP's root. The runtime must match the workers' operating system and processor, and executable files must keep their executable permissions. ZIPs currently support regular files and directories, without symbolic links.

The dashboard has the same upload option, with one argument per line. Instances show `preparing` while their files are downloaded and unpacked; status updates continue during that work. Process restarts reuse the prepared files. Deleting a service removes its instance directories; these files are not managed persistent state.

## A service with stored data

The first collection is a dictionary with `Get`, `Put`, and `Delete`. It stores JSON values under string keys. Each service has its own dictionary, copied across three workers. Each partition has a leader that orders its changes, and a write returns success after at least two workers have recorded it on disk. Reads also go through that leader and confirm it can still reach a majority.

To try it on your computer, keep the coordinator running and start three workers in separate terminals:

```sh
fabrenetes worker --id state-one --state-listen 127.0.0.1:9101
fabrenetes worker --id state-two --state-listen 127.0.0.1:9102
fabrenetes worker --id state-three --state-listen 127.0.0.1:9103
```

From the repository root, build the [dictionary application](examples/dictionary/main.go) and create a service:

```sh
go build -o /tmp/fabrenetes-dictionary ./examples/dictionary
fabrenetes create --name notes --stateful --partitioning hash --partitions 2 --port 9001 /tmp/fabrenetes-dictionary
fabrenetes services
```

`--stateful` selects three copies automatically. All three workers must be registered before creating the service. Use the service's address to store and read a value:

```sh
curl -X PUT -H 'Content-Type: application/json' -d '"hello"' 'http://127.0.0.1:8080/apps/notes/value?key=message'
curl 'http://127.0.0.1:8080/apps/notes/value?key=message'
```

Choose `--partitioning none` for one partition, or `--partitioning hash --partitions <count>` to divide keys into a fixed number of partitions. The hash of a key always chooses the same partition. Each partition has its own replicated log and leader; all partitions currently use the service's same three workers. The strategy and count stay fixed after creation. Omitting these flags gives one partition.

The copies share the dictionary, so the value can be read through another copy's port. Stopping an application and letting it restart keeps its data. Stopping one worker leaves two that can continue after choosing a leader. Restart that worker with the same ID, state address, and data directory to let it catch up. Three workers on one computer demonstrate the protocol; separate machines are needed to survive losing a whole computer.

Fabrenetes passes each application a local `state_url`. The Go client reads it with `collections.New("")`; other languages can use `GET`, `PUT`, and `DELETE` at `${state_url}/values?key=...`. `PUT` takes a JSON body, `GET` returns that JSON or 404 for a missing key, and `DELETE` removes the key. Operations affect one key at a time; there are no transactions yet. A failed or timed-out write may still have committed, so the client does not automatically retry it.

For workers in VMs, listen on an address other workers can reach, such as `--state-listen 0.0.0.0:9101 --state-address 192.168.2.9:9101`. The state address is that worker's own host and port. All three addresses must be reachable from the other workers. These settings can also come from the optional `state_listen` and `state_address` environment variables, including in the worker's Linux environment file.

Service definitions are saved by the coordinator. Worker data is stored separately from application files and survives agent restarts. Both commands accept `--data-dir` to choose where their data goes; otherwise they use the operating system's application configuration directory. Deleting a service stops its applications and closes its dictionary, but retains the dictionary files on disk. Creating a service with the same name creates a fresh dictionary.

## CLI and API

The `coordinator` and `worker` commands run those programs. Other commands call the coordinator's HTTP API:

| CLI command | API request | What it does |
| --- | --- | --- |
| `fabrenetes workers` | `GET /workers` | Lists workers, their IP addresses, and status. |
| `fabrenetes create` | `POST /services` | Creates a service from a command and a number of copies. |
| `fabrenetes services` | `GET /services` | Shows each copy's placement, port, process ID, status, restart count, and service address. |
| `fabrenetes delete <name>` | `DELETE /services/{name}` | Removes a service and tells its workers to stop its applications. |

Workers register at `POST /workers` and send updates to `POST /workers/{id}/heartbeat`. The reply tells them what to run. The dashboard shares this API; see [server.go](internal/coordinator/server.go).

Package uploads use `POST /packages`; workers download them through `GET /packages/{id}`.

Requests to `/apps/{name}/...` are forwarded to that application's matching path. For example, `/apps/notes/value?key=message` reaches `/value?key=message`. Responses include `Fabrenetes-Worker` and `Fabrenetes-Instance` headers so you can see which copy handled a request.

## Where to look

The dashboard shows applications by worker and lets you create or delete services, including stateful services with single or hash partitioning. Its stored-data view shows each partition's last reported leader, worker roles, and availability. Service links use the stable address. **Start worker** and **Stop worker** also control agents in configured Linux VMs while leaving the VMs running.

The [clock application](demo/clock) can be used to demo Fabrenetes. Its `GET /time` response includes UTC time, the worker and service, and the instance and process IDs. Use the worker's IP and displayed port to check which copy responds.

To follow the code, start here:

1. [cmd/fabrenetes/main.go](cmd/fabrenetes/main.go) handles commands; [services.go](cmd/fabrenetes/services.go) sends service requests.
2. [internal/api/api.go](internal/api/api.go) defines the messages they send.
3. [internal/coordinator/server.go](internal/coordinator/server.go) receives requests; [services.go](internal/coordinator/services.go) chooses where applications run; [routing.go](internal/coordinator/routing.go) balances application requests. [vms.go](internal/coordinator/vms.go) handles VM commands.
4. [internal/worker/worker.go](internal/worker/worker.go) receives assignments; [packages.go](internal/worker/packages.go) downloads and unpacks applications; [processes.go](internal/worker/processes.go) starts and restarts them; [ports.go](internal/worker/ports.go) chooses ports.
5. [collections/dictionary.go](collections/dictionary.go) is the application client; [internal/state/](internal/state/) stores and replicates dictionaries; [internal/coordinator/state.go](internal/coordinator/state.go) saves service definitions and chooses stateful workers.

The other folders are:

- [internal/coordinator/dashboard/](internal/coordinator/dashboard/): the web dashboard.
- [demo/](demo/): a clock application that can be used to demo Fabrenetes.
- [examples/](examples/): sample echo and dictionary applications.
- [deploy/](deploy/): the Linux setup file for running a worker in the background.

## Checks

Run these from the repository root:

```sh
go test -race ./...
go vet ./...
```

The tests cover worker registration, application placement, package delivery, process recovery, port allocation, VM commands, request balancing, and partitioned dictionary recovery. The race check looks for unsafe access to shared data; `go vet` checks for likely Go mistakes.

## Current limits

Applications run as ordinary processes, using either uploaded packages or programs already installed on the workers. An unreachable worker's applications are not automatically moved elsewhere. Dictionaries use a fixed group of three workers and need two available workers to serve requests. Changing that group, moving partitions between worker groups, and transactions are future work. Replication uses [HashiCorp Raft](https://github.com/hashicorp/raft). The coordinator is the single entry point for balanced requests. Containers are also future work.
