package imports

import (
	"sync"
	"testing"
)

func TestImportJob_Import(t *testing.T) {
	job := ImportJob{
		Source: Source{
			FoldName:       "../test_file",
			DbListFileName: "database_list.txt",
		},
		Dest: Dest{
			Username: "root",
			Pwd:      "",
			Host:     "127.0.0.1",
			Port:     "4000",
		},
		Wg: &sync.WaitGroup{},
	}

	job.Parse()
	job.Import()

	t.Error("---")
}
