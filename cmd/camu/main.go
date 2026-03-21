package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/maksim/camu/internal/config"
	"github.com/maksim/camu/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: camu <serve|test>\n")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "serve":
		configPath := "camu.yaml"
		if len(os.Args) > 2 {
			for i := 2; i < len(os.Args); i++ {
				if os.Args[i] == "--config" && i+1 < len(os.Args) {
					configPath = os.Args[i+1]
					i++
				}
			}
		}

		cfg, err := config.Load(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
			os.Exit(1)
		}

		srv, err := server.New(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating server: %v\n", err)
			os.Exit(1)
		}

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		if err := srv.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
			os.Exit(1)
		}
		slog.Info("server started", "address", srv.Address())

		// Block until signal
		<-sigCh
		slog.Info("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			slog.Error("shutdown error", "error", err)
		}

	case "test":
		// Parse flags
		endpoint := ""
		category := ""
		instances := "1"
		bench := false
		chaos := false
		duration := ""
		authToken := ""

		for i := 2; i < len(os.Args); i++ {
			switch os.Args[i] {
			case "--endpoint":
				i++
				endpoint = os.Args[i]
			case "--category":
				i++
				category = os.Args[i]
			case "--instances":
				i++
				instances = os.Args[i]
			case "--bench":
				bench = true
			case "--chaos":
				chaos = true
			case "--duration":
				i++
				duration = os.Args[i]
			case "--auth-token":
				i++
				authToken = os.Args[i]
			}
		}

		// Build go test command
		args := []string{"test"}
		tags := []string{"integration"}
		if chaos {
			tags = append(tags, "chaos")
		}
		args = append(args, "-tags", strings.Join(tags, ","))

		if bench {
			args = append(args, "-bench=.", "./test/bench/")
		} else {
			args = append(args, "./test/integration/")
		}

		if category != "" {
			args = append(args, "-run", category)
		}

		args = append(args, "-timeout", "300s", "-v")

		// Set env vars for test configuration
		env := os.Environ()
		if endpoint != "" {
			env = append(env, "CAMU_TEST_ENDPOINT="+endpoint)
		}
		env = append(env, "CAMU_TEST_INSTANCES="+instances)
		if authToken != "" {
			env = append(env, "CAMU_TEST_AUTH_TOKEN="+authToken)
		}
		if duration != "" {
			env = append(env, "CAMU_TEST_DURATION="+duration)
		}

		cmd := exec.Command("go", args...)
		cmd.Env = env
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			os.Exit(1)
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}
