//go:build linux

package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

type processIdentity struct {
	StartToken string
	ArgvHash   string
	PGID       int
}

func newProcessCommand(argv []string) (*exec.Cmd, error) {
	prlimit, err := exec.LookPath("prlimit")
	if err != nil {
		return nil, fmt.Errorf("Linux process runtime requires prlimit for bounded output: %w", err)
	}
	args := make([]string, 0, len(argv)+2)
	args = append(args, "--fsize=4194304", "--")
	args = append(args, argv...)
	return exec.Command(prlimit, args...), nil
}

func setProcessGroup(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}

func inspectProcess(pid int) (processIdentity, error) {
	var id processIdentity
	if pid <= 0 {
		return id, errors.New("invalid process ID")
	}
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return id, err
	}
	close := strings.LastIndexByte(string(stat), ')')
	if close < 0 || close+2 >= len(stat) {
		return id, errors.New("malformed proc stat")
	}
	fields := strings.Fields(string(stat[close+2:])) // state is field 3
	if len(fields) <= 19 {
		return id, errors.New("short proc stat")
	}
	if fields[0] == "Z" || fields[0] == "X" {
		return id, errors.New("process has exited")
	}
	pgid, err := strconv.Atoi(fields[2]) // field 5
	if err != nil {
		return id, err
	}
	id.PGID = pgid
	id.StartToken = fields[19] // field 22, clock ticks since boot
	cmdline, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return processIdentity{}, err
	}
	args := strings.Split(strings.TrimSuffix(string(cmdline), "\x00"), "\x00")
	if len(args) == 1 && args[0] == "" {
		return processIdentity{}, errors.New("empty process argv")
	}
	b, _ := json.Marshal(args)
	sum := sha256.Sum256(b)
	id.ArgvHash = hex.EncodeToString(sum[:])
	return id, nil
}

func processExited(pid int) bool {
	if pid <= 0 {
		return true
	}
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	if err != nil {
		return false
	}
	close := strings.LastIndexByte(string(stat), ')')
	if close < 0 {
		return true
	}
	fields := strings.Fields(string(stat[close+2:]))
	return len(fields) > 0 && (fields[0] == "Z" || fields[0] == "X")
}

func processMatches(pid, pgid int, startToken, expectedArgvHash string) (bool, error) {
	id, err := inspectProcess(pid)
	if err != nil {
		return false, err
	}
	return id.PGID == pgid && id.StartToken == startToken && id.ArgvHash == expectedArgvHash, nil
}

func findProcessByToken(token string, argv []string) (int, processIdentity, bool) {
	var empty processIdentity
	if token == "" {
		return 0, empty, false
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, empty, false
	}
	want := []byte("GPUBROKER_ATTEMPT_TOKEN=" + token + "\x00")
	expected := argvHash(argv)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		id, err := inspectProcess(pid)
		if err != nil || id.ArgvHash != expected {
			continue
		}
		env, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "environ"))
		if err == nil && bytes.Contains(env, want) {
			return pid, id, true
		}
	}
	return 0, empty, false
}

func signalProcessGroup(pgid int, sig syscall.Signal) error {
	if pgid <= 0 {
		return errors.New("invalid process group ID")
	}
	return syscall.Kill(-pgid, sig)
}

func processGroupMatches(pgid int, token string) bool {
	if pgid <= 0 || token == "" {
		return false
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	want := []byte("GPUBROKER_ATTEMPT_TOKEN=" + token + "\x00")
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if err != nil {
			continue
		}
		close := strings.LastIndexByte(string(stat), ')')
		if close < 0 {
			continue
		}
		fields := strings.Fields(string(stat[close+2:]))
		if len(fields) > 2 && fields[0] != "Z" && fields[0] != "X" {
			group, err := strconv.Atoi(fields[2])
			if err == nil && group == pgid {
				env, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "environ"))
				if err == nil && bytes.Contains(env, want) {
					return true
				}
			}
		}
	}
	return false
}

func processGroupAlive(pgid int) bool {
	if pgid <= 0 {
		return false
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return syscall.Kill(-pgid, 0) == nil
	}
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if err != nil {
			continue
		}
		close := strings.LastIndexByte(string(stat), ')')
		if close < 0 {
			continue
		}
		fields := strings.Fields(string(stat[close+2:]))
		if len(fields) <= 2 || fields[0] == "Z" || fields[0] == "X" {
			continue
		}
		group, err := strconv.Atoi(fields[2])
		if err == nil && group == pgid {
			return true
		}
	}
	return false
}
