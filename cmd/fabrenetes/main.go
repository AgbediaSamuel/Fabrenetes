package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"fabrenetes/internal/api"
	"fabrenetes/internal/coordinator"
	"fabrenetes/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := run(ctx, os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	err = errorHandler(err, func(err error) {
		fmt.Fprintln(os.Stderr, "error:", err)
	})
	if err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("expected a command (use --help)")
	}
	switch args[0] {
	case "coordinator":
		return runCoordinator(ctx, args[1:])
	case "worker":
		return runWorker(ctx, args[1:])
	case "workers":
		return listWorkers(ctx, args[1:])
	case "create":
		return createService(ctx, args[1:])
	case "services":
		return listServices(ctx, args[1:])
	case "delete":
		return deleteService(ctx, args[1:])
	case "help", "--help", "-h":
		fmt.Println("Usage: fabrenetes <command> [flags]\n\nCommands:\n  coordinator  Run the coordinator\n  worker       Run a worker agent\n  workers      List workers\n  create       Create a service from an executable\n  services     List services and their processes\n  delete       Stop and remove a service\n\nUse fabrenetes <command> --help for flags.")
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runCoordinator(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP address to listen on")
	timeout := flags.Duration("worker-timeout", 5*time.Second, "time without a heartbeat before a worker is unreachable")
	workerVMs := flags.String("worker-vms", "", "comma-separated VM names whose agents the dashboard can start and stop")
	vmListCommand := flags.String("vm-list-command", "", "VM discovery executable (defaults to Multipass)")
	vmRunCommand := flags.String("vm-run-command", "", "VM command runner executable (defaults to Multipass)")
	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("coordinator takes flags only")
	}
	if *timeout <= 0 {
		return errors.New("worker-timeout must be positive")
	}
	cluster := coordinator.New(*timeout)
	if *workerVMs != "" {
		err = cluster.ConfigureVMs(ctx, strings.Split(*workerVMs, ","), coordinator.VMCommands{
			List: *vmListCommand,
			Run:  *vmRunCommand,
		})
		if err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	server := &http.Server{
		Handler:           cluster.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	result := make(chan error, 1)
	go func() { result <- server.Serve(listener) }()
	fmt.Println("Coordinator listening on", listener.Addr())
	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		fmt.Println("Coordinator stopping")
		return server.Shutdown(shutdownCtx)
	}
}

func runWorker(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	id := flags.String("id", "", "unique worker identity (required)")
	address := coordinatorFlag(flags)
	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("worker takes flags only")
	}
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("read hostname: %w", err)
	}
	return worker.Run(ctx, worker.Config{
		ID: *id, Hostname: hostname, CoordinatorURL: *address,
		HeartbeatInterval: time.Second, RequestTimeout: 3 * time.Second,
	})
}

func listWorkers(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("workers", flag.ContinueOnError)
	address := coordinatorFlag(flags)
	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("workers takes flags only")
	}
	var result api.WorkersResponse
	err = coordinatorRequest(ctx, http.MethodGet, *address, "workers", nil, &result)
	if err != nil {
		return err
	}
	writer := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(writer, "WORKER\tIP\tSTATUS\tLAST SEEN (UTC)")
	for _, item := range result.Workers {
		seen := "-"
		if !item.LastSeen.IsZero() {
			seen = item.LastSeen.UTC().Format("29 Apr 2006 15:04:05")
		}
		fmt.Fprintf(writer, "%s\t%s\t%s\t%s\n", item.ID, item.IP, item.Status, seen)
	}
	return writer.Flush()
}

func coordinatorFlag(flags *flag.FlagSet) *string {
	address := os.Getenv("coordinator_url")
	if address == "" {
		address = "http://127.0.0.1:8080"
	}
	return flags.String("coordinator", address, "coordinator base URL")
}

func errorHandler(err error, onError func(error)) error {
	if err == nil {
		return nil
	}
	if onError != nil {
		onError(err)
	}
	return err
}
