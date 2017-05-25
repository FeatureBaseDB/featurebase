package ctl

import (
	"testing"
	"golang.org/x/net/context"
)

func TestBackupCommand_FileRequired(t *testing.T){
	cm := BackupCommand{}
	ctx := context.Background()
	err := cm.Run(ctx)
	if err.Error() != "output file required"{
		t.Fatalf("Command not working, expect: output file required, actual: '%s'", err)
	}
}
