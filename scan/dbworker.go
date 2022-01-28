package scan

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

type DbWorkerPool struct {
	WorkerName string

	//
	maxWorker      int
	queuedWorkTask chan DbWorkTask

	wait sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	currentWorker      int
	currentWorkerMutex sync.Mutex

	dbConnectionString string
}

func NewDbWorker(ctx context.Context, maxWorker int, dbConnectionString string) *DbWorkerPool {

	workerCtx, cancel := context.WithCancel(ctx)

	return &DbWorkerPool{
		maxWorker:          maxWorker,
		queuedWorkTask:     make(chan DbWorkTask),
		wait:               sync.WaitGroup{},
		currentWorker:      0,
		currentWorkerMutex: sync.Mutex{},

		ctx:    workerCtx,
		cancel: cancel,

		dbConnectionString: dbConnectionString,
	}
}

func (w *DbWorkerPool) dbConnection() (*sql.DB, error) {
	db, err := sql.Open("mysql", w.dbConnectionString)
	return db, err
}

func (w *DbWorkerPool) Run() WorkerRunResult {

	workerRunResult := WorkerRunResult{
		MakeWorkerCount: 0,
		WorkerError:     make([]WorkerError, 0),
	}

	for i := 0; i < w.maxWorker; i++ {

		w.wait.Add(1)
		go func(workerId int) {
			defer w.wait.Done()

			defer fmt.Printf("[%v][%v] Stop Worker\n", w.WorkerName, workerId)

			db, err := w.dbConnection()
			if err != nil {
				fmt.Printf("[error][dbConnection] %v\n", err)
				return
			}
			defer db.Close()

			workerRunResult.MakeWorkerCount++

			//db.SetConnMaxLifetime(time.Second * 3)
			//db.SetMaxIdleConns(1024)
			//db.SetMaxOpenConns(10000)
			//db.Ping()

			//fmt.Printf("[%v][%v] Start Worker\n", w.WorkerName, workerId)

			for {
				select {
				case <-w.ctx.Done():
					return
				case workTask, ok := <-w.queuedWorkTask:
					if !ok {
						return
					}

					func() {
						w.IncrementDoWoker()
						defer w.DecrementDoWorker()

						workTask.Task(db, workTask.Arguments...)
						//fmt.Printf("[conn] %v\n", db.Stats().OpenConnections)

					}()
				}
			}
		}(i)
	}

	// worker가 모두 일했는지 체크
	go func() {
		<-w.ctx.Done()
		close(w.queuedWorkTask)
	}()

	return workerRunResult
}

func (w *DbWorkerPool) Done() {
	w.cancel()
}

func (w *DbWorkerPool) IsDone() bool {
	if w.ctx.Err() != nil {
		return true
	}

	return false
}

func (w *DbWorkerPool) Wait() {
	w.wait.Wait()
}

func (w *DbWorkerPool) IncrementDoWoker() {
	w.currentWorkerMutex.Lock()
	defer w.currentWorkerMutex.Unlock()

	w.currentWorker++
}

func (w *DbWorkerPool) DecrementDoWorker() {
	w.currentWorkerMutex.Lock()
	defer w.currentWorkerMutex.Unlock()

	w.currentWorker--
}

func (w *DbWorkerPool) GetCurrentAliveWorker() int {
	w.currentWorkerMutex.Lock()
	defer w.currentWorkerMutex.Unlock()

	return w.currentWorker
}

func (w *DbWorkerPool) AddTask(task DbWorkTask) {
	defer func() {
		recover()
	}()

	if w.IsDone() {
		return
	}

	w.queuedWorkTask <- task
}
