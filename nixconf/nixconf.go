// Package nixconf is the setup check: it asks Nix for its configuration,
// decides whether Nix would use this narrenhaus as a cache, and writes the
// advice to print: the lines that are missing, the file they belong in and
// why, and how to upload.
//
// The check reads and suggests. It writes nothing, edits no file and asks
// for no privilege.
//
// Three things make the setup complete: narrenhaus's URL among the
// substituters, its signing key among the trusted public keys, and
// fallback = true, without which a narrenhaus that is not running fails
// Nix operations. The first two count only where the Nix daemon honours
// them: for a user it trusts that is the user's whole configuration, for
// any other user the files under /etc/nix alone. fallback is sent by the
// client on every connection, so it always counts from the user's
// configuration.
package nixconf

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// systemConf is Nix's system configuration file, the place of last
	// resort for the advice.
	systemConf = "/etc/nix/nix.conf"
	// customConf is where Determinate Nix, which owns nix.conf, takes the
	// machine's own settings from.
	customConf = "/etc/nix/nix.custom.conf"

	// timeLimit bounds all runs of nix of one check, so that a daemon that
	// does not answer cannot keep narrenhaus from starting.
	timeLimit = 20 * time.Second

	// maxErrorLen bounds the text of an error quoted in the advice.
	maxErrorLen = 200
)

// daemonEnv makes nix print the configuration as the daemon sees it: no
// user files (the variable set, even to nothing, replaces them) and no
// NIX_CONFIG.
var daemonEnv = []string{"NIX_USER_CONF_FILES=", "NIX_CONFIG"}

// Env is what the check reads from the machine; tests replace it.
type Env struct {
	// Run runs nix with args and extra environment (entries "K=V"; an
	// entry "K" without '=' removes K) and returns standard output.
	Run func(ctx context.Context, env []string, args ...string) ([]byte, error)
	// ReadFile and Readlink read /etc/nix files.
	ReadFile func(path string) ([]byte, error)
	Readlink func(path string) (string, error)
	Home     string // the user's home directory
	// ConfigHome is XDG_CONFIG_HOME, under which Nix looks for the user's
	// nix/nix.conf. Empty means Home/.config.
	ConfigHome string
	// UserConfFiles is NIX_USER_CONF_FILES, and UserConfFilesSet whether
	// the variable is set at all. Nix then reads the user's settings from
	// the files it names, separated by colons, the first having the last
	// word, and from none under ConfigHome.
	UserConfFiles    string
	UserConfFilesSet bool
}

// System is the Env of the real machine.
func System() Env {
	home, _ := os.UserHomeDir()
	files, set := os.LookupEnv("NIX_USER_CONF_FILES")
	return Env{
		Run:              runNix,
		ReadFile:         os.ReadFile,
		Readlink:         os.Readlink,
		Home:             home,
		ConfigHome:       os.Getenv("XDG_CONFIG_HOME"),
		UserConfFiles:    files,
		UserConfFilesSet: set,
	}
}

// runNix runs the nix on PATH and returns its standard output. Standard
// error is not part of the output; when nix fails, its error line is added
// to the error.
func runNix(ctx context.Context, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "nix", args...)
	cmd.Env = environ(os.Environ(), env)
	// Do not wait for ever on a child of nix that keeps the pipes open.
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		if said := errorLine(exit.Stderr); said != "" {
			return nil, fmt.Errorf("%w: %s", err, said)
		}
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// errorLine picks from what nix wrote to standard error the line to quote:
// the first that begins with "error:", or else the first that is not empty.
func errorLine(stderr []byte) string {
	first := ""
	for line := range strings.Lines(string(stderr)) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "error:") {
			return line
		}
		if first == "" {
			first = line
		}
	}
	return first
}

// environ returns base with changes applied: an entry "K=V" sets K, an
// entry "K" removes it. base is not modified.
func environ(base, changes []string) []string {
	out := make([]string, 0, len(base)+len(changes))
	for _, kv := range base {
		name, _, _ := strings.Cut(kv, "=")
		changed := false
		for _, c := range changes {
			if cn, _, _ := strings.Cut(c, "="); cn == name {
				changed = true
				break
			}
		}
		if !changed {
			out = append(out, kv)
		}
	}
	for _, c := range changes {
		if strings.Contains(c, "=") {
			out = append(out, c)
		}
	}
	return out
}

// Input is what narrenhaus is started with.
type Input struct {
	Listen    string // the --listen value
	PublicKey string // "<name>:<base64>"
}

// Report is the result of the check.
type Report struct {
	Configured bool
	// Text is what to print: one line when configured, otherwise the
	// missing lines, where they go and why; always the upload hint. It
	// ends with a newline.
	Text string
}

// URL is the substituter URL for a listen address: "http://" + address,
// with an unspecified host replaced by 127.0.0.1.
func URL(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "http://" + listen
	}
	if host == "" {
		host = "127.0.0.1"
	} else if addr, err := netip.ParseAddr(host); err == nil && addr.IsUnspecified() {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// loopback reports whether only this machine can reach the listen address.
// A host name other than localhost is not known to be loopback.
func loopback(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		host = listen
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.Unmap().IsLoopback()
}

// settings are the three settings the check reads.
type settings struct {
	substituters []string
	keys         []string // trusted-public-keys, in Nix's order
	fallback     bool
}

// nixCommand prefixes every run of nix: the commands used are experimental
// in the releases that have them.
var nixCommand = []string{"--extra-experimental-features", "nix-command"}

// readSettings reads the configuration nix reports when run with the given
// changes to the environment. `nix config show` is tried first and
// `nix show-config`, its name on older Nix, when that fails.
func readSettings(ctx context.Context, env Env, changes []string) (settings, error) {
	commands := [][]string{{"config", "show", "--json"}, {"show-config", "--json"}}
	var failures []string
	for _, command := range commands {
		out, err := env.Run(ctx, changes, append(append([]string(nil), nixCommand...), command...)...)
		if err == nil {
			var s settings
			if s, err = parseSettings(out); err == nil {
				return s, nil
			}
		}
		failures = append(failures, oneLine(err.Error()))
	}
	// Which of the two failures says what is wrong depends on the Nix:
	// one of them may only be "no such command".
	text := "nix " + strings.Join(commands[0], " ") + ": " + failures[0]
	if failures[1] != failures[0] {
		text += "; nix " + strings.Join(commands[1], " ") + ": " + failures[1]
	}
	return settings{}, errors.New(text)
}

// parseSettings reads the output of `nix config show --json`: an object of
// setting name to an object whose "value" is the setting's value.
func parseSettings(out []byte) (settings, error) {
	var all map[string]struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(out, &all); err != nil {
		return settings{}, fmt.Errorf("the output is not understood: %v", err)
	}
	var s settings
	for _, read := range []struct {
		name string
		into any
	}{
		{"substituters", &s.substituters},
		{"trusted-public-keys", &s.keys},
		{"fallback", &s.fallback},
	} {
		setting, ok := all[read.name]
		if !ok || len(setting.Value) == 0 {
			return settings{}, fmt.Errorf("the output has no setting %q", read.name)
		}
		// A null would be read as any type, and as nothing.
		if string(setting.Value) == "null" {
			return settings{}, fmt.Errorf("the output's setting %q is not understood: its value is null", read.name)
		}
		if err := json.Unmarshal(setting.Value, read.into); err != nil {
			return settings{}, fmt.Errorf("the output's setting %q is not understood: %v", read.name, err)
		}
	}
	return s, nil
}

// trusted asks the daemon whether it trusts the user. The answer's field
// is true or false on current Nix and 1 or 0 on older; a missing field, an
// answer that cannot be read and a failing command all mean not trusted.
func trusted(ctx context.Context, env Env) bool {
	out, err := env.Run(ctx, nil, append(append([]string(nil), nixCommand...), "store", "info", "--json")...)
	if err != nil {
		return false
	}
	var info struct {
		Trusted json.RawMessage `json:"trusted"`
	}
	if json.Unmarshal(out, &info) != nil {
		return false
	}
	switch string(bytes.TrimSpace(info.Trusted)) {
	case "true", "1":
		return true
	}
	return false
}

// hasSubstituter reports whether one of the substituters is the cache at
// url. An entry is compared after dropping its parameters and a trailing
// slash, which Nix does not count as part of a cache's address.
func hasSubstituter(substituters []string, url string) bool {
	for _, entry := range substituters {
		entry, _, _ = strings.Cut(entry, "?")
		if strings.TrimSuffix(entry, "/") == url {
			return true
		}
	}
	return false
}

// findKey looks for key ("<name>:<base64>") among the trusted public keys.
// Nix keeps only the first key of a name, so entries with key's name and
// another key that come before it make it useless: those are returned as
// conflicts, all of them when key is not there at all.
func findKey(keys []string, key string) (present bool, conflicts []string) {
	name, _, _ := strings.Cut(key, ":")
	for _, entry := range keys {
		if entry == key {
			return true, conflicts
		}
		if entryName, _, ok := strings.Cut(entry, ":"); ok && entryName == name {
			conflicts = append(conflicts, entry)
		}
	}
	return false, conflicts
}

// place is where the substituter and the key belong.
type place int

const (
	placeUser      place = iota // the user's nix.conf: the daemon trusts the user
	placeGenerated              // /etc/nix/nix.conf is written by NixOS or nix-darwin
	placeCustom                 // /etc/nix/nix.custom.conf, included by nix.conf
	placeSystem                 // /etc/nix/nix.conf
)

// systemPlace finds where a user the daemon does not trust has to put the
// lines; the second result is the target of a generated nix.conf.
func systemPlace(env Env) (place, string) {
	if target, err := env.Readlink(systemConf); err == nil && generated(target) {
		return placeGenerated, target
	}
	if conf, err := env.ReadFile(systemConf); err == nil && includesCustom(conf) {
		return placeCustom, ""
	}
	return placeSystem, ""
}

// generated reports whether a link's target is in the Nix store or in
// /etc/static, where NixOS and nix-darwin keep the files they generate.
func generated(target string) bool {
	return strings.HasPrefix(target, "/nix/store/") || strings.HasPrefix(target, "/etc/static/")
}

// includesCustom reports whether a nix.conf has an include or !include
// line naming nix.custom.conf. Lines are read as Nix reads them: a comment
// starts at '#', and words are separated by white space.
func includesCustom(conf []byte) bool {
	for line := range strings.Lines(string(conf)) {
		line, _, _ = strings.Cut(line, "#")
		words := strings.Fields(line)
		if len(words) == 2 && (words[0] == "include" || words[0] == "!include") && path.Base(words[1]) == path.Base(customConf) {
			return true
		}
	}
	return false
}

// userConf is the user's own nix.conf, as a path to print: the first file
// NIX_USER_CONF_FILES names, or the one under ConfigHome. read is false
// when the variable is set and names no file: Nix then reads none of the
// user's, and the path is the one it would read without the variable.
func (env Env) userConf() (file string, read bool) {
	if env.UserConfFilesSet {
		// Nix leaves out the empty parts.
		for file := range strings.SplitSeq(env.UserConfFiles, ":") {
			if file != "" {
				return file, true
			}
		}
	}
	switch {
	case env.ConfigHome != "":
		file = filepath.Join(env.ConfigHome, "nix", "nix.conf")
	case env.Home != "":
		file = filepath.Join(env.Home, ".config", "nix", "nix.conf")
	default:
		file = "~/.config/nix/nix.conf"
	}
	return file, !env.UserConfFilesSet
}

// Check reads the machine's Nix configuration and reports whether Nix
// would use the narrenhaus of in as a cache, with the text to print.
func Check(ctx context.Context, env Env, in Input) Report {
	env = env.complete()
	ctx, cancel := context.WithTimeout(ctx, timeLimit)
	defer cancel()

	a := advice{url: URL(in.Listen), key: in.PublicKey}
	a.userConf, a.userConfRead = env.userConf()
	configured := a.examine(ctx, env)

	a.paragraph("To upload a store path: nix copy --to '" + a.url + "?compression=none' <path>\n" +
		"A very large path may need --option stalled-download-timeout <seconds>.")
	if !loopback(in.Listen) {
		a.paragraph("Warning: narrenhaus listens on " + in.Listen + ", which is not a loopback address: " +
			"anyone who can reach the port can upload into the shared prefix and get narinfos signed with this machine's key.")
	}
	return Report{Configured: configured, Text: a.text.String()}
}

// complete returns env with every function set; one that was missing
// fails.
func (env Env) complete() Env {
	if env.Run == nil {
		env.Run = func(context.Context, []string, ...string) ([]byte, error) {
			return nil, errors.New("no way to run nix was given")
		}
	}
	if env.ReadFile == nil {
		env.ReadFile = func(string) ([]byte, error) { return nil, errors.ErrUnsupported }
	}
	if env.Readlink == nil {
		env.Readlink = func(string) (string, error) { return "", errors.ErrUnsupported }
	}
	return env
}

// advice builds the text of a report.
type advice struct {
	url      string // narrenhaus's substituter URL
	key      string // narrenhaus's public key, "<name>:<base64>"
	userConf string // the user's nix.conf
	// userConfRead is false when NIX_USER_CONF_FILES keeps Nix from
	// reading any file of the user's.
	userConfRead bool
	text         strings.Builder
}

// paragraph adds s to the text, after an empty line unless it is the
// first.
func (a *advice) paragraph(s string) {
	if a.text.Len() > 0 {
		a.text.WriteByte('\n')
	}
	a.text.WriteString(s)
	a.text.WriteByte('\n')
}

// lines adds an indented block of lines to copy.
func (a *advice) lines(lines ...string) {
	a.paragraph("    " + strings.Join(lines, "\n    "))
}

// userLines adds the lines that belong in the user's nix.conf, with what
// comes before the file's name, and says so when Nix does not read it.
func (a *advice) userLines(before string, lines ...string) {
	a.paragraph(before + a.userConf + ", which needs neither root nor a restart:")
	a.lines(lines...)
	if !a.userConfRead {
		a.paragraph("NIX_USER_CONF_FILES is set and names no file, so Nix reads no configuration file of this user's: " +
			"the lines count once the variable is unset or names " + a.userConf + ".")
	}
}

func (a *advice) substituterLine() string { return "extra-substituters = " + a.url }
func (a *advice) keyLine() string         { return "extra-trusted-public-keys = " + a.key }

const (
	fallbackLine = "fallback = true"
	// fallbackReason is the one sentence on what the line is for and what
	// else it changes.
	fallbackReason = "fallback = true lets Nix carry on when narrenhaus is not running, " +
		"and it makes a failed download from any cache a local build instead of an error."
)

// examine reads the configuration, writes the status part of the text and
// reports whether Nix is configured.
func (a *advice) examine(ctx context.Context, env Env) bool {
	full, err := readSettings(ctx, env, nil)
	if err != nil {
		a.unread(err)
		return false
	}

	// The view the daemon honours for the substituter and the key: the
	// user's own when it trusts the user, the system files' otherwise.
	// fallback is always the user's.
	isTrusted := trusted(ctx, env)
	view := full
	if !isTrusted {
		if view, err = readSettings(ctx, env, daemonEnv); err != nil {
			a.unread(err)
			return false
		}
	}
	hasSub := hasSubstituter(view.substituters, a.url)
	listed, conflicts := findKey(view.keys, a.key)
	hasKey := listed && len(conflicts) == 0

	if hasSub && hasKey && full.fallback {
		a.configured(ctx, env, isTrusted)
		return true
	}

	if hasSub && hasKey {
		a.paragraph("Nix uses narrenhaus at " + a.url + " as a cache, but fallback is off.")
	} else {
		a.paragraph("Nix is not set up to use narrenhaus at " + a.url + " as a cache.")
	}
	if len(conflicts) > 0 {
		name, _, _ := strings.Cut(a.key, ":")
		ahead := ":"
		if listed {
			ahead = " ahead of narrenhaus's:"
		}
		a.paragraph("trusted-public-keys holds another key named " + name + ahead)
		a.lines(conflicts...)
		a.paragraph("Nix keeps only the first key of a name, so narrenhaus's signatures are refused until that entry is removed.")
	}

	// The lines the daemon has to see. The key line is wanted when the
	// key is not listed at all; a conflict alone is mended by removing.
	var missing, options []string
	if !hasSub {
		missing = append(missing, a.substituterLine())
		options = append(options, `nix.settings.extra-substituters = [ "`+a.url+`" ];`)
	}
	if !listed {
		missing = append(missing, a.keyLine())
		options = append(options, `nix.settings.extra-trusted-public-keys = [ "`+a.key+`" ];`)
	}

	switch {
	case len(missing) == 0:
	case isTrusted:
		// One file takes all of it.
		if !full.fallback {
			missing = append(missing, fallbackLine)
		}
		a.userLines("The Nix daemon trusts this user. Add to ", missing...)
		if !full.fallback {
			a.paragraph(fallbackReason)
		}
		return false
	default:
		why := "The Nix daemon does not trust this user, so it takes these settings from the system configuration alone"
		// Say so when the user's own files hold what the daemon's view
		// lacks, or the advice reads as if it had not looked.
		if userListed, _ := findKey(full.keys, a.key); (!hasSub && hasSubstituter(full.substituters, a.url)) || (!listed && userListed) {
			why += "; the lines in the user's own configuration do not count"
		}
		why += ". "
		switch where, target := systemPlace(env); where {
		case placeGenerated:
			a.paragraph(why + systemConf + " is generated (a link to " + target + "). " +
				"Add to the NixOS or nix-darwin configuration, rebuild, and see that the Nix daemon has been restarted:")
			a.lines(options...)
		case placeCustom:
			a.paragraph(why + "Add to " + customConf + " as root, then restart the Nix daemon:")
			a.lines(missing...)
		default:
			a.paragraph(why + "Add to " + systemConf + " as root, then restart the Nix daemon:")
			a.lines(missing...)
		}
	}
	if !full.fallback {
		a.userLines("Add to ", fallbackLine)
		a.paragraph(fallbackReason)
	}
	return false
}

// configured writes the one line of a complete setup and, where the setup
// rests on a file under /etc/nix, that the daemon must have been restarted
// since the edit: the check sees the files, not the daemon's own settings.
func (a *advice) configured(ctx context.Context, env Env, isTrusted bool) {
	line := "Nix is set up to use narrenhaus at " + a.url + " as a cache."
	const (
		effect  = "they take effect once the Nix daemon has been restarted after the edit, which this check cannot see."
		inEtc   = "\nThe settings are in a file under /etc/nix: " + effect
		ifInEtc = "\nIf the settings are in a file under /etc/nix, " + effect
	)
	if !isTrusted {
		// Only the system files were counted.
		line += inEtc
	} else {
		// A trusted user's settings may be in either place; the system
		// files' view tells which.
		system, err := readSettings(ctx, env, daemonEnv)
		present, _ := findKey(system.keys, a.key)
		switch {
		case err != nil:
			line += ifInEtc
		case present || hasSubstituter(system.substituters, a.url):
			line += inEtc
		}
	}
	a.paragraph(line)
}

// unread writes the advice for a configuration that could not be read:
// every line, for /etc/nix/nix.conf, which works for every user. err is
// one of readSettings, which is one line of bounded length.
func (a *advice) unread(err error) {
	a.paragraph("The Nix configuration could not be read (" + err.Error() + "), so nothing was checked. " +
		"For Nix to use narrenhaus at " + a.url + " as a cache, " + systemConf + " needs these lines, and the Nix daemon a restart after the edit:")
	a.lines(a.substituterLine(), a.keyLine(), fallbackLine)
	a.paragraph(fallbackReason)
}

// oneLine returns the first line of s, cut to maxErrorLen bytes.
func oneLine(s string) string {
	s, _, _ = strings.Cut(strings.TrimSpace(s), "\n")
	s = strings.TrimSpace(s)
	if len(s) <= maxErrorLen {
		return s
	}
	cut := maxErrorLen
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
