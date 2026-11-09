//go:build darwin

package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

type processIdentity struct {
	StartToken string
	ArgvHash   string
	PGID       int
}

func newProcessCommand(argv []string) (*exec.Cmd, error) {
	return exec.Command(argv[0], argv[1:]...), nil
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
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return id, err
	}
	if kp.Proc.P_stat == 5 || kp.Proc.P_stat == 6 { // SZOMB / SEXIT
		return id, errors.New("process has exited")
	}
	id.PGID = int(kp.Eproc.Pgid)
	id.StartToken = fmt.Sprintf("%d:%d", kp.Proc.P_starttime.Sec, kp.Proc.P_starttime.Usec)
	argsData, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return processIdentity{}, err
	}
	if len(argsData) < 4 {
		return processIdentity{}, errors.New("short kern.procargs2 response")
	}
	argc := int(binary.NativeEndian.Uint32(argsData[:4]))
	if argc < 1 || argc > processMaxArgs {
		return processIdentity{}, fmt.Errorf("invalid process argc %d", argc)
	}
	data := argsData[4:]
	execEnd := bytes.IndexByte(data, 0)
	if execEnd < 0 {
		return processIdentity{}, errors.New("malformed process executable path")
	}
	data = data[execEnd+1:]
	for len(data) > 0 && data[0] == 0 { // kernel padding before argv
		data = data[1:]
	}
	args := make([]string, 0, argc)
	for len(args) < argc {
		end := bytes.IndexByte(data, 0)
		if end < 0 {
			return processIdentity{}, errors.New("truncated process argv")
		}
		args = append(args, string(data[:end]))
		data = data[end+1:]
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
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	return err != nil || kp.Proc.P_stat == 5 || kp.Proc.P_stat == 6 // SZOMB / SEXIT
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
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return 0, empty, false
	}
	expected := argvHash(argv)
	want := []byte("GPUBROKER_ATTEMPT_TOKEN=" + token)
	for _, kp := range procs {
		pid := int(kp.Proc.P_pid)
		id, err := inspectProcess(pid)
		if err != nil || id.ArgvHash != expected {
			continue
		}
		if processHasToken(pid, token, want) {
			return pid, id, true
		}
	}
	return 0, empty, false
}

func processGroupMatches(pgid int, token string) bool {
	if pgid <= 0 || token == "" {
		return false
	}
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return false
	}
	want := []byte("GPUBROKER_ATTEMPT_TOKEN=" + token)
	for _, kp := range procs {
		if int(kp.Eproc.Pgid) != pgid || kp.Proc.P_stat == 5 || kp.Proc.P_stat == 6 {
			continue
		}
		if processHasToken(int(kp.Proc.P_pid), token, want) {
			return true
		}
	}
	return false
}

func processHasToken(pid int, token string, want []byte) bool {
	args, err := unix.SysctlRaw("kern.procargs2", pid)
	if err == nil && bytes.Contains(args, want) {
		return true
	}
	output, err := exec.Command("ps", "eww", "-p", fmt.Sprint(pid), "-o", "command=").Output()
	return err == nil && bytes.Contains(output, []byte("GPUBROKER_ATTEMPT_TOKEN="+token))
}

func processGroupAlive(pgid int) bool {
	if pgid <= 0 {
		return false
	}
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return syscall.Kill(-pgid, 0) == nil
	}
	for _, kp := range procs {
		if int(kp.Eproc.Pgid) == pgid && kp.Proc.P_stat != 5 && kp.Proc.P_stat != 6 { // SZOMB / SEXIT
			return true
		}
	}
	return false
}

func signalProcessGroup(pgid int, sig syscall.Signal) error {
	if pgid <= 0 {
		return errors.New("invalid process group ID")
	}
	return syscall.Kill(-pgid, sig)
}
