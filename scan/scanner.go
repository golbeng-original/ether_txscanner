package scan

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/umbracle/go-web3"
	"github.com/umbracle/go-web3/jsonrpc"
	"github.com/vtok/ether_txscanner/scan/logger"
)

func blockQuery(client *jsonrpc.Client, arguments ...interface{}) {

	if len(arguments) == 0 {
		fmt.Println("argument empty")
		return
	}

	blockIndex := arguments[0].(uint64)
	//txStream := arguments[1].(chan *web3.Transaction)
	dbRequestStream := arguments[1].(chan interface{})
	dbConfirmStream := arguments[2].(chan interface{})
	blockCacheState := arguments[3].(*BlockCacheState)

	if blockCacheState.IsCompletedBlock(blockIndex) {
		logger.CatchedBlockCompleteLog(blockIndex)
		return
	}

	var block *web3.Block
	var err error

	reRequestCount := 0 // 10회 재 요청
	for reRequestCount < 10 {
		block, err = client.Eth().GetBlockByNumber(web3.BlockNumber(blockIndex), true)

		if err != nil {

			if IsRetryableWeb3Error(err) {
				fmt.Println("web3 client retry error : ", err)

				reRequestCount++
				time.Sleep(200 * time.Millisecond)
				continue
			}

			fmt.Println("[error]web3 client error : ", err)
			break
		}

		if block == nil {
			reRequestCount++
			time.Sleep(200 * time.Millisecond)
			continue
		}

		break
	}

	if block == nil {
		fmt.Printf("[error]block[%d] scan error\n", blockIndex)
		return
	}

	// block 기록 (DB 기록)
	dbRequestStream <- &dbNewBlock{
		blockIndex: blockIndex,
		blockHash:  block.Hash,
		timestamp:  time.Unix(int64(block.Timestamp), 0).UTC(),
		txCount:    len(block.Transactions),
	}

	// Cache에 [완료 안 된 block등록]
	blockCacheState.RegisterUncompletedBlock(block)

	// transaction들 Scan
	for _, transaction := range block.Transactions {
		//txStream <- transaction

		transactionScan(client, transaction, dbRequestStream, dbConfirmStream, blockCacheState)
	}
}

func transactionScan(client *jsonrpc.Client, tx *web3.Transaction, dbRequestStream chan<- interface{}, dbConfirmStream chan<- interface{}, blockCacheState *BlockCacheState) {

	if blockCacheState.IsCompletedTx(tx.Hash) {
		logger.CatchedTxCompleteLog(&tx.Hash)

		// Cache에 [완료 안 된 block에 Transaction 체크]
		needConfirm := blockCacheState.CheckUncompletedBlock(tx.BlockNumber, &tx.Hash)
		if needConfirm {
			dbConfirmStream <- BlockConfirm(tx.BlockNumber)
		}
		return
	}

	// db 상에서는 완료가 된 상태 이므로.. web3에서 정보 가져올 필요 없음..
	if blockCacheState.IsCompleteInCacheTx(tx.Hash) {

		// confirmStream에 전송
		dbConfirmStream <- TxConform{
			blockIndex: tx.BlockNumber,
			txHash:     tx.Hash,
		}
		return
	}

	var receipt *web3.Receipt
	var err error

	reRequestCount := 0 // 10회 재 요청
	for reRequestCount < 10 {
		receipt, err = client.Eth().GetTransactionReceipt(tx.Hash)

		if err != nil {
			if IsRetryableWeb3Error(err) {
				fmt.Println("web3 client retry error : ", err)

				time.Sleep(200 * time.Millisecond)
				reRequestCount++
				continue
			}

			fmt.Println("web3 client error : ", err)
			break
		}

		if receipt == nil {
			time.Sleep(200 * time.Millisecond)
			reRequestCount++
			continue
		}

		break
	}

	if receipt == nil {
		fmt.Printf("[Error][transaction reciept] [block = %v][tx = %v] scan failed\n", tx.BlockNumber, tx.Hash.String())
		return
	}

	// transaction 추가 기록
	dbRequestStream <- &dbNewTxReceiptContent{
		blockIndex:  tx.BlockNumber,
		transaction: tx,
		receipt:     receipt,
	}

}

func dbWrite(db *sql.DB, arguments ...interface{}) {

	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r)
		}
	}()

	if len(arguments) == 0 {
		fmt.Println("[error]argument empty")
		return
	}

	dbConfirmStream := arguments[1].(chan interface{})
	blockCacheState := arguments[2].(*BlockCacheState)

	switch arguments[0].(type) {
	case *dbNewBlock:
		wrtieNewBlock(db, arguments[0].(*dbNewBlock))
	case *dbNewTxReceiptContent:
		receiptContent := arguments[0].(*dbNewTxReceiptContent)
		isSuccess := writeNewTx(db, blockCacheState, receiptContent)
		if isSuccess {
			// Cache에 [완료 안 된 block에 Transaction 체크]
			needConfirm := blockCacheState.CheckUncompletedBlock(receiptContent.blockIndex, &receiptContent.transaction.Hash)
			if needConfirm {
				dbConfirmStream <- BlockConfirm(receiptContent.blockIndex)
			}
		} else {
			fmt.Println("writeNewTx sucess = ", isSuccess)
		}
	}
}

func dbConfirmBlock(db *sql.DB, arguments ...interface{}) {

	blockCacheState := arguments[1].(*BlockCacheState)

	switch arguments[0].(type) {
	case BlockConfirm:

		blockIndex := arguments[0].(BlockConfirm)
		isConfirm, err := confirmBlock(db, uint64(blockIndex))
		if err != nil {
			fmt.Println("[error][confirmBlock] : ", err)
			return
		}

		if isConfirm {
			logger.BlockCompleteLog(uint64(blockIndex))

			// 완료 확인했으니 제거
			blockCacheState.CompleteUnCompletedBlock(uint64(blockIndex))
		}
	case TxConform:
		txConfirm := arguments[0].(TxConform)

		isTxConfirm, err := confirmTx(db, &txConfirm.txHash)
		if err != nil {
			fmt.Println("[error][confirmBlock] : ", err)
			return
		}

		if isTxConfirm {
			logger.TxCompleteLog(&txConfirm.txHash)

			blockIndex := txConfirm.blockIndex

			// Cache에 [완료 안 된 block에 Transaction 체크]
			needConfirm := blockCacheState.CheckUncompletedBlock(blockIndex, &txConfirm.txHash)
			if needConfirm {

				// Block 확인
				isBlockConfirm, err := confirmBlock(db, uint64(blockIndex))
				if err != nil {
					fmt.Println("[error][confirmBlock] : ", err)
					return
				}

				if isBlockConfirm {
					logger.BlockCompleteLog(blockIndex)

					// 완료 확인했으니 제거
					blockCacheState.CompleteUnCompletedBlock(uint64(blockIndex))
				}
			}
		}
	}
}

type Web3Recorder struct {
	BlockScanWorker int
	DbWriteWorker   int
	DbConfirmWorker int

	BlockWeb3Providers []string
	DBConnectionString string
}

func (r *Web3Recorder) BlockScan(ctx context.Context, fromBlock uint64, toBlock uint64) error {

	blockCacheState, err := NewBlockCacheState(
		r.DBConnectionString,
		uint(fromBlock),
		uint(toBlock),
	)

	if err != nil {
		return err
	}

	fmt.Println("[cache]load complete")

	// Worker 할당
	blockScanWorker := NewWeb3ScanWorker(ctx, r.BlockScanWorker, r.BlockWeb3Providers)
	blockScanWorker.WorkerName = "BlockScanWorker"
	blockScannerResult := blockScanWorker.Run()

	if blockScannerResult.IsHasWorkerError() {

		errorContent := ""
		for _, workerError := range blockScannerResult.WorkerError {
			errorContent += fmt.Sprintf("%v\n", workerError.Error)
		}

		return fmt.Errorf("%v", errorContent)
	}

	dbWriteWorker := NewDbWorker(ctx, r.DbWriteWorker, r.DBConnectionString)
	dbWriteWorker.WorkerName = "DbWriteWorker"
	dbWriteResult := dbWriteWorker.Run()

	if dbWriteResult.IsHasWorkerError() {
		errorContent := ""
		for _, workerError := range blockScannerResult.WorkerError {
			errorContent += fmt.Sprintf("%v\n", workerError.Error)
		}

		return fmt.Errorf("%v", errorContent)
	}

	dbConfirmWorker := NewDbWorker(ctx, r.DbConfirmWorker, r.DBConnectionString)
	dbConfirmWorker.WorkerName = "DbConfirmWorker"
	dbConfirmResult := dbConfirmWorker.Run()

	if dbConfirmResult.IsHasWorkerError() {
		errorContent := ""
		for _, workerError := range blockScannerResult.WorkerError {
			errorContent += fmt.Sprintf("%v\n", workerError.Error)
		}

		return fmt.Errorf("%v", errorContent)
	}

	//
	fmt.Println("BlockScanWorker Count = ", blockScannerResult.MakeWorkerCount)
	fmt.Println("DBWriteWorker Count = ", blockScannerResult.MakeWorkerCount)
	fmt.Println("DBConfirmWorker Count = ", blockScannerResult.MakeWorkerCount)
	//

	dbWriteRequestStream := make(chan interface{})
	dbConfirmBlockStream := make(chan interface{})

	wg := sync.WaitGroup{}

	// Block Scan Worker 수행
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := fromBlock; i <= toBlock; i++ {

			workerArgument := NewWeb3WorkTask(i, dbWriteRequestStream, dbConfirmBlockStream, blockCacheState)
			workerArgument.Task = blockQuery

			blockScanWorker.AddTask(workerArgument)
		}

		blockScanWorker.Done()
		blockScanWorker.Wait()
		close(dbWriteRequestStream)

	}()

	// DB Write worker 수행
	wg.Add(1)
	go func() {
		defer wg.Done()

		for dbWriteRequest := range dbWriteRequestStream {

			workerArgument := NewDbWorkTask(dbWriteRequest, dbConfirmBlockStream, blockCacheState)
			workerArgument.Task = dbWrite
			dbWriteWorker.AddTask(workerArgument)
		}

		dbWriteWorker.Done()
		dbWriteWorker.Wait()

		close(dbConfirmBlockStream)
	}()

	// DB Block Confirm Worker 수행
	wg.Add(1)
	go func() {
		defer wg.Done()

		for dbConfirmRequest := range dbConfirmBlockStream {

			workerArgument := NewDbWorkTask(dbConfirmRequest, blockCacheState)
			workerArgument.Task = dbConfirmBlock
			dbConfirmWorker.AddTask(workerArgument)
		}

		dbConfirmWorker.Done()
		dbConfirmWorker.Wait()
	}()

	wg.Wait()

	return nil
}
