package sigtrap

import (
	"os"
	"os/signal"
	"syscall"
)

// Send signal to the current process
func signalSelf(sig os.Signal) error {
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return p.Signal(sig)
}

// Signal 13 observed in prod because we're piping logs to jq in a bash script.
// Trying to just translate it to sigint here.
func TrapSignal13() {
	go func() {
		shutdown := make(chan os.Signal, 1)
		signal.Notify(shutdown, syscall.SIGPIPE)
		for {
			<-shutdown
			_ = signalSelf(os.Interrupt)
		}
	}()
}
