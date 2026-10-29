package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/amber-store/narrenhaus/narinfo"
	"github.com/amber-store/narrenhaus/nixconf"
)

// check is the command check: it writes to stdout what the Nix
// configuration lacks to use a narrenhaus that listens at opts.Listen, or
// that it lacks nothing. Lacking something is no error.
//
// The advice names the signing key, so check makes that key when there is
// none yet, as serve would. It needs nothing else: no server, no prefix, no
// cache.
func check(ctx context.Context, opts options, stdout io.Writer) error {
	signing, err := narinfo.LoadOrCreateSigningKey(filepath.Join(opts.ConfigDir, signingKeyFile))
	if err != nil {
		return fmt.Errorf("--config-dir: %w", err)
	}
	report := nixconf.Check(ctx, nixconf.System(), nixconf.Input{Listen: opts.Listen, PublicKey: signing.Public()})
	if _, err := io.WriteString(stdout, report.Text); err != nil {
		return fmt.Errorf("writing the advice: %w", err)
	}
	return nil
}
