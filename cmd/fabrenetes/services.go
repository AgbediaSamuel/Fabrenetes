package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"fabrenetes/internal/api"
)

func createService(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("create", flag.ContinueOnError)
	name := flags.String("name", "", "service name (required)")
	replicas := flags.Int("replicas", 1, "number of processes to run")
	port := flags.Int("port", 0, "starting port; each worker picks an available port per instance")
	address := coordinatorFlag(flags)
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: fabrenetes create [flags] executable [args...]")
		flags.PrintDefaults()
	}
	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if !api.ValidName(*name) || *replicas < 1 || flags.NArg() == 0 {
		return errors.New("create requires a valid name, positive replica count, and executable")
	}
	if *port < 0 || *port > 65535 {
		return errors.New("port must be between 1 and 65535 when supplied")
	}

	spec := api.ServiceSpec{
		Name:     *name,
		Command:  flags.Arg(0),
		Args:     flags.Args()[1:],
		Replicas: *replicas,
		Port:     *port,
	}
	var service api.Service
	err = coordinatorRequest(ctx, http.MethodPost, *address, "services", spec, &service)
	if err != nil {
		return err
	}
	fmt.Printf("Created %s with %d replicas\n", service.Name, service.Replicas)
	return nil
}

func listServices(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("services", flag.ContinueOnError)
	address := coordinatorFlag(flags)
	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("services takes flags only")
	}
	var result api.ServicesResponse
	err = coordinatorRequest(ctx, http.MethodGet, *address, "services", nil, &result)
	if err != nil {
		return err
	}
	writer := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(writer, "SERVICE\tWORKER\tPORT\tPID\tSTATUS\tRESTARTS\tERROR")
	for _, service := range result.Services {
		for _, instance := range service.Instances {
			port := "-"
			if instance.Port > 0 {
				port = strconv.Itoa(instance.Port)
			}
			fmt.Fprintf(writer, "%s\t%s\t%s\t%d\t%s\t%d\t%s\n", service.Name, instance.WorkerID, port, instance.PID, instance.Status, instance.Restarts, instance.Error)
		}
	}
	return writer.Flush()
}

func deleteService(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("delete", flag.ContinueOnError)
	address := coordinatorFlag(flags)
	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if flags.NArg() != 1 || !api.ValidName(flags.Arg(0)) {
		return errors.New("delete requires a service name after its flags")
	}
	path := "services/" + url.PathEscape(flags.Arg(0))
	err = coordinatorRequest(ctx, http.MethodDelete, *address, path, nil, nil)
	if err != nil {
		return err
	}
	fmt.Println("Deleted", flags.Arg(0))
	return nil
}

func coordinatorRequest(ctx context.Context, method, address, path string, body, result any) error {
	endpoint, err := url.JoinPath(address, path)
	if err != nil {
		return err
	}
	var buffer bytes.Buffer
	if body != nil {
		err = json.NewEncoder(&buffer).Encode(body)
		if err != nil {
			return err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, &buffer)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode >= 300 {
		var failure api.ErrorResponse
		err = json.Unmarshal(data, &failure)
		if err == nil && failure.Error != "" {
			return errors.New(failure.Error)
		}
		return fmt.Errorf("coordinator returned %s", response.Status)
	}
	if result == nil {
		return nil
	}
	return json.Unmarshal(data, result)
}
