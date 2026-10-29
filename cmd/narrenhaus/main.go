// Command narrenhaus is a Nix binary cache that runs on the machine it
// serves and keeps what it is given on a jaccard-store server.
//
//	narrenhaus serve --server ENDPOINT_ID --prefix PREFIX [--listen ADDRESS] [--cache-dir DIR] [--cache-size SIZE] [--config-dir DIR] [--priority N]
//	narrenhaus check [--listen ADDRESS] [--config-dir DIR]
//
// Every option is a flag and a NARRENHAUS_… environment variable; the flag
// wins.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/amber-store/core/reference"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/urfave/cli/v2"
)

// version is the release this binary was built as. A release build sets it
// with -ldflags "-X main.version=..."; any other build is "dev".
var version = "dev"

func main() {
	if err := newApp(os.Stdout, os.Stderr).RunContext(untilSignalled(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "narrenhaus:", err)
		os.Exit(1)
	}
}

// untilSignalled returns a context that ends with the first SIGINT or
// SIGTERM, on which the command winds down. A second signal ends the
// process at once.
//
// The signals stay the command's for that. Given back to the system after
// the first, they would be what the process was started with, and a shell
// starts the background jobs of a script with SIGINT ignored: the second
// signal would then do nothing.
func untilSignalled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		cancel()
		<-signals
		fmt.Fprintln(os.Stderr, "narrenhaus: a second signal: stopped without waiting for the requests that run")
		os.Exit(1)
	}()
	return ctx
}

// options is what the flags and the environment say, checked.
type options struct {
	// Server is the jaccard-store server's endpoint ID.
	Server irohkey.EndpointID
	// Prefix starts every reference name, verbatim.
	Prefix string
	// Listen is the HTTP address, host:port.
	Listen string
	// CacheDir holds the pack cache.
	CacheDir string
	// CacheSize is the pack cache's limit in bytes, never zero.
	CacheSize uint64
	// ConfigDir holds the client's key and the signing key.
	ConfigDir string
	// Priority is the Priority of nix-cache-info.
	Priority int
}

// Defaults of the options that have one.
const (
	defaultListen    = "127.0.0.1:8484"
	defaultCacheSize = "10GiB"
	defaultPriority  = 50
)

// userDir returns narrenhaus's directory under one of the user's base
// directories, or nothing when the system names no such directory; the
// option must then be given.
func userDir(base func() (string, error)) string {
	dir, err := base()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "narrenhaus")
}

// optionFlags returns the options as flags, new ones on every call: a
// command keeps what it parsed in its flags.
func optionFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "server", EnvVars: []string{"NARRENHAUS_SERVER"},
			Usage: "`ENDPOINT_ID` of the jaccard-store server; serve requires it"},
		&cli.StringFlag{Name: "prefix", EnvVars: []string{"NARRENHAUS_PREFIX"},
			Usage: "`PREFIX` of the reference names, taken verbatim, so it wants its own trailing separator; serve requires it"},
		&cli.StringFlag{Name: "listen", EnvVars: []string{"NARRENHAUS_LISTEN"}, Value: defaultListen,
			Usage: "HTTP `ADDRESS` Nix talks to, host:port"},
		&cli.StringFlag{Name: "cache-dir", EnvVars: []string{"NARRENHAUS_CACHE_DIR"}, Value: userDir(os.UserCacheDir),
			Usage: "`DIR`ectory of the pack cache"},
		&cli.StringFlag{Name: "cache-size", EnvVars: []string{"NARRENHAUS_CACHE_SIZE"}, Value: defaultCacheSize,
			Usage: "`SIZE` the pack cache is kept under: bytes, or a number with KiB, MiB, GiB or TiB"},
		&cli.StringFlag{Name: "config-dir", EnvVars: []string{"NARRENHAUS_CONFIG_DIR"}, Value: userDir(os.UserConfigDir),
			Usage: "`DIR`ectory of the client's key and the signing key, created when missing"},
		&cli.IntFlag{Name: "priority", EnvVars: []string{"NARRENHAUS_PRIORITY"}, Value: defaultPriority,
			Usage: "Priority in nix-cache-info: Nix asks the substituter with the lower `NUMBER` first"},
	}
}

// newApp returns the command.
func newApp(stdout, stderr io.Writer) *cli.App {
	return &cli.App{
		Name:            "narrenhaus",
		Version:         version,
		Usage:           "a local Nix binary cache that keeps its contents on a jaccard-store server",
		Writer:          stdout,
		ErrWriter:       stderr,
		HideHelpCommand: true,
		Commands: []*cli.Command{
			{
				Name:  "serve",
				Usage: "say what the Nix configuration lacks, then serve the binary cache",
				Description: "serve writes to standard error: what the Nix configuration lacks to use it,\n" +
					"the address it serves at, then a line for each upload and each NAR served.\n" +
					"It serves whether Nix is set up for it or not. On SIGINT or SIGTERM it stops\n" +
					"once the requests that are running have finished, or after ten seconds.",
				Flags: optionFlags(),
				Action: func(c *cli.Context) error {
					opts, err := loadOptions(c)
					if err != nil {
						return err
					}
					return serve(c.Context, opts, c.App.ErrWriter, thisHost())
				},
			},
			{
				Name:  "check",
				Usage: "say what the Nix configuration lacks to use narrenhaus, and exit",
				Description: "check reads --listen and --config-dir alone. It takes the other options\n" +
					"so that the command line of serve can be given to it as it is.",
				Flags: optionFlags(),
				Action: func(c *cli.Context) error {
					opts, err := loadCheckOptions(c)
					if err != nil {
						return err
					}
					return check(c.Context, opts, c.App.Writer)
				},
			},
		},
	}
}

// loadOptions reads and checks every option, for serve. Each error is one
// line that names the option at fault.
func loadOptions(c *cli.Context) (options, error) {
	opts, err := loadCheckOptions(c)
	if err != nil {
		return options{}, err
	}

	server := c.String("server")
	if server == "" {
		return options{}, errors.New("--server is required: the endpoint ID of the jaccard-store server")
	}
	if opts.Server, err = irohkey.ParseEndpointID(server); err != nil {
		return options{}, fmt.Errorf("--server %q is not an endpoint ID: %w", server, err)
	}

	opts.Prefix = c.String("prefix")
	if opts.Prefix == "" {
		return options{}, errors.New("--prefix is required: the prefix of the reference names")
	}
	if err := validatePrefix(opts.Prefix); err != nil {
		return options{}, err
	}

	if opts.CacheDir = c.String("cache-dir"); opts.CacheDir == "" {
		return options{}, errors.New("--cache-dir is empty: name the directory of the pack cache")
	}
	size := c.String("cache-size")
	if opts.CacheSize, err = parseSize(size); err != nil {
		return options{}, fmt.Errorf("--cache-size %q: %w", size, err)
	}
	opts.Priority = c.Int("priority")
	return opts, nil
}

// loadCheckOptions reads and checks the options check needs: the address
// and the configuration directory. The others stay zero.
func loadCheckOptions(c *cli.Context) (options, error) {
	var opts options
	opts.Listen = c.String("listen")
	if err := validateListen(opts.Listen); err != nil {
		return options{}, err
	}
	if opts.ConfigDir = c.String("config-dir"); opts.ConfigDir == "" {
		return options{}, errors.New("--config-dir is empty: name the directory of the keys")
	}
	return opts, nil
}

// validatePrefix checks that both names a store path gets with the prefix
// are reference names: the narinfo's, the prefix and 32 characters of the
// store path's hash, and the content's, the prefix and "nar/" and 52
// characters of the NAR's hash. The hashes add nothing a name may not
// have, so zeros stand for them.
func validatePrefix(prefix string) error {
	for _, name := range []string{
		prefix + strings.Repeat("0", 32) + ".narinfo",
		prefix + "nar/" + strings.Repeat("0", 52) + ".nar",
	} {
		if err := reference.ValidateName(name); err != nil {
			return fmt.Errorf("--prefix %q makes no valid reference name: %w", prefix, err)
		}
	}
	return nil
}

// validateListen checks that the address is host:port with a port number.
// The host may be empty, for every address of the machine.
func validateListen(listen string) error {
	if listen == "" {
		return errors.New("--listen is empty: give the HTTP address as host:port")
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("--listen %q is not host:port: %w", listen, err)
	}
	// The address goes into the Nix configuration as a URL, where a
	// service name cannot stand for the port.
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("--listen %q: the port %q is not a number from 0 to 65535", listen, port)
	}
	return nil
}

// sizeUnits are the units parseSize knows, with their bytes.
var sizeUnits = []struct {
	suffix string
	bytes  uint64
}{
	{"KiB", 1 << 10},
	{"MiB", 1 << 20},
	{"GiB", 1 << 30},
	{"TiB", 1 << 40},
	{"B", 1},
}

// parseSize reads a size in bytes: a whole number alone ("1073741824"), or
// one with a binary unit right after it ("10GiB", "500MiB"; KiB, MiB, GiB,
// TiB, or B). Zero is an error, since a cache of no size holds nothing,
// and so is a size that does not fit in 64 bits.
func parseSize(s string) (uint64, error) {
	digits, unit := s, uint64(1)
	for _, u := range sizeUnits {
		if rest, ok := strings.CutSuffix(s, u.suffix); ok {
			digits, unit = rest, u.bytes
			break
		}
	}
	// ParseUint takes a sign for an error, as it does anything else that
	// is not a digit.
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, errors.New("the size does not fit in 64 bits")
		}
		return 0, errors.New("want a whole number of bytes, or one with KiB, MiB, GiB or TiB, as in 10GiB")
	}
	if n == 0 {
		return 0, errors.New("the size must not be zero")
	}
	if n > math.MaxUint64/unit {
		return 0, errors.New("the size does not fit in 64 bits")
	}
	return n * unit, nil
}
