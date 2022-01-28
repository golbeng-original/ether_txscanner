package scan

import (
	"database/sql"

	"github.com/umbracle/go-web3/jsonrpc"
)

type WorkerError struct {
	WorkerId int
	Error    error
}

type WorkerRunResult struct {
	MakeWorkerCount int
	WorkerError     []WorkerError
}

func (result *WorkerRunResult) IsHasWorkerError() bool {
	return len(result.WorkerError) > 0
}

type Web3WorkTask struct {
	Task      func(*jsonrpc.Client, ...interface{})
	Arguments []interface{}
}

func NewWeb3WorkTask(arguments ...interface{}) Web3WorkTask {
	workArgument := Web3WorkTask{
		Arguments: make([]interface{}, 0),
	}

	workArgument.Arguments = append(workArgument.Arguments, arguments...)

	return workArgument
}

type DbWorkTask struct {
	Task      func(*sql.DB, ...interface{})
	Arguments []interface{}
}

func NewDbWorkTask(arguments ...interface{}) DbWorkTask {
	workArgument := DbWorkTask{
		Arguments: make([]interface{}, 0),
	}

	workArgument.Arguments = append(workArgument.Arguments, arguments...)

	return workArgument
}

type IWorkerPool interface {
	Run()
	Done()
	IsDone() bool
	Wait()

	AddTask(argument Web3WorkTask)
}
