package scan

import (
	"context"
	"fmt"
	"sync"

	"github.com/umbracle/go-web3/jsonrpc"
)

type createWeb3ClientResult struct {
	client *jsonrpc.Client
	err    error
}

func web3ProviderRepeat(done <-chan interface{}, web3Providers ...string) <-chan string {

	web3ProviderStream := make(chan string)

	go func() {
		defer func() {
			close(web3ProviderStream)
		}()

		for {
			for _, web3Provider := range web3Providers {

				select {
				case <-done:
					return
				case web3ProviderStream <- web3Provider:
				}
			}
		}
	}()

	return web3ProviderStream
}

func createWeb3Client(web3ProviderStream <-chan string, maxRequestCount int) <-chan createWeb3ClientResult {

	jsonrpcClientGen := func() (*jsonrpc.Client, error) {

		var client *jsonrpc.Client
		var err error
		//var pearCount uint64

		// 30 회 요청 한다.
		requestCount := 0
		for requestCount < maxRequestCount {

			web3Provider, ok := <-web3ProviderStream
			if !ok {
				return nil, fmt.Errorf("web3ProviderStream closed")
			}

			client, err = jsonrpc.NewClient(web3Provider)
			if err != nil {
				return nil, err
			}

			break

			/*
				pearCount, err = client.Net().PeerCount()
				if err != nil {
					return nil, err
				}

				if pearCount > 0 {
					break
				}*/
		}

		//if pearCount == 0 {
		//	return nil, fmt.Errorf("jsonrpc.Client Pear Count zero")
		//}

		return client, err
	}

	result := make(chan createWeb3ClientResult)

	go func() {

		client, err := jsonrpcClientGen()

		result <- createWeb3ClientResult{
			client: client,
			err:    err,
		}

	}()

	return result

}

type Web3ScanWorkerPool struct {
	WorkerName string

	//
	maxWorker      int
	queuedWorkTask chan Web3WorkTask

	wait sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	currentWorker      int
	currentWorkerMutex sync.Mutex

	web3Providers []string
}

func NewWeb3ScanWorker(ctx context.Context, maxWorker int, web3Providers []string) *Web3ScanWorkerPool {

	workerCtx, cancel := context.WithCancel(ctx)

	return &Web3ScanWorkerPool{
		maxWorker:      maxWorker,
		queuedWorkTask: make(chan Web3WorkTask),
		wait:           sync.WaitGroup{},

		ctx:    workerCtx,
		cancel: cancel,

		currentWorker:      0,
		currentWorkerMutex: sync.Mutex{},

		web3Providers: web3Providers,
	}
}

func (w *Web3ScanWorkerPool) Run() WorkerRunResult {

	workerRunResultMutex := sync.Mutex{}
	workerRunResult := WorkerRunResult{
		MakeWorkerCount: 0,
		WorkerError:     make([]WorkerError, 0),
	}

	repeatDoneChan := make(chan interface{})
	web3ProviderStream := web3ProviderRepeat(repeatDoneChan, w.web3Providers...)

	wg := sync.WaitGroup{}

	for i := 0; i < w.maxWorker; i++ {

		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			resultChan := createWeb3Client(web3ProviderStream, 30)

			result := <-resultChan

			if result.err != nil {
				func() {
					workerRunResultMutex.Lock()
					defer workerRunResultMutex.Unlock()

					workerRunResult.WorkerError = append(workerRunResult.WorkerError, WorkerError{
						WorkerId: workerId,
						Error:    result.err,
					})

				}()
				return
			}

			workerRunResult.MakeWorkerCount++

			w.wait.Add(1)
			go func() {
				defer w.wait.Done()
				defer result.client.Close()

				defer fmt.Printf("[%v][%v]Stop Worker\n", w.WorkerName, workerId)

				for {
					select {
					case <-w.ctx.Done():
						return
					case workTask, ok := <-w.queuedWorkTask:
						if !ok {
							//fmt.Printf("[%v][%v]Close Worker\n", w.WorkerName, workerId)
							return
						}

						func() {
							w.IncrementDoWoker()
							defer w.DecrementDoWorker()

							workTask.Task(result.client, workTask.Arguments...)
						}()
					}
				}
			}()
		}(i)
	}

	wg.Wait()

	// worker가 모두 일했는지 체크
	go func() {
		<-w.ctx.Done()
		close(w.queuedWorkTask)
	}()

	return workerRunResult
}

func (w *Web3ScanWorkerPool) Done() {
	w.cancel()
}

func (w *Web3ScanWorkerPool) IsDone() bool {
	if w.ctx.Err() != nil {
		return true
	}

	return false
}

func (w *Web3ScanWorkerPool) Wait() {
	w.wait.Wait()
}

func (w *Web3ScanWorkerPool) IncrementDoWoker() {
	w.currentWorkerMutex.Lock()
	defer w.currentWorkerMutex.Unlock()

	w.currentWorker++
}

func (w *Web3ScanWorkerPool) DecrementDoWorker() {
	w.currentWorkerMutex.Lock()
	defer w.currentWorkerMutex.Unlock()

	w.currentWorker--
}

func (w *Web3ScanWorkerPool) GetCurrentAliveWorker() int {
	w.currentWorkerMutex.Lock()
	defer w.currentWorkerMutex.Unlock()

	return w.currentWorker
}

func (w *Web3ScanWorkerPool) AddTask(task Web3WorkTask) {
	defer func() {
		recover()
	}()

	if w.IsDone() {
		return
	}

	w.queuedWorkTask <- task

}
