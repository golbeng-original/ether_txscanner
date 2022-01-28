package scan

import "fmt"

type BlockScanConfigure struct {
	DbConnString    string
	Web3Providers   []string
	BlockScanWorker int
	DBWriteWorker   int
	DBConfirmWorker int
}

func (config *BlockScanConfigure) Err() error {

	if len(config.DbConnString) == 0 {
		return fmt.Errorf("DbConnString is empty")
	}

	if len(config.Web3Providers) == 0 {
		return fmt.Errorf("Web3Providers is empty")
	}

	return nil
}
