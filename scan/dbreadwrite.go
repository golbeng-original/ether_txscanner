package scan

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/umbracle/go-web3"
	"github.com/vtok/ether_txscanner/scan/logger"
)

type dbNewBlock struct {
	blockIndex uint64
	blockHash  web3.Hash
	timestamp  time.Time

	txCount int
}

type dbNewTxReceiptContent struct {
	blockIndex  uint64
	transaction *web3.Transaction
	receipt     *web3.Receipt
}

func readUnit(db *sql.DB, fromBlock uint, toBlock uint, queryFunc func(uint, uint)) *sync.WaitGroup {

	wg := sync.WaitGroup{}

	// connection 당 100000 Row로 나눠서 Load
	const UNIT_BLOCK_RANGE_COUNT = 200000
	unitFromBlock := fromBlock
	for unitFromBlock <= toBlock {

		unitToBlock := unitFromBlock + UNIT_BLOCK_RANGE_COUNT
		if unitToBlock >= toBlock {
			unitToBlock = toBlock
		}

		//fmt.Printf("make read Unity %v - %v\n", unitFromBlock, unitToBlock)

		wg.Add(1)
		go func(unitFrom uint, unitTo uint) {
			defer wg.Done()

			queryFunc(unitFrom, unitTo)
		}(unitFromBlock, unitToBlock)

		unitFromBlock = unitToBlock + 1
	}

	return &wg
}

func readCompletedBlocks(db *sql.DB, fromBlock uint, toBlock uint) (<-chan uint64, <-chan error, error) {

	errorStream := make(chan error)
	blockIndicesStream := make(chan uint64)

	queryFunc := func(from uint, to uint) {

		completedBlockQuery := "SELECT block_index FROM block WHERE scaned_transaction = true "
		completedBlockQuery += "AND block_index >= ? AND block_index <= ?"

		//rows, err := db.Query(completedBlockQuery)
		rows, err := db.Query(completedBlockQuery, from, to)
		if err != nil {
			fmt.Println("[error][readCompletedBlocks]")
			return
		}

		defer rows.Close()

		for rows.Next() {

			var blockIndex uint
			err := rows.Scan(&blockIndex)
			if err != nil {
				fmt.Printf("[error][readCompletedBlocks] [%v]", err)
				continue
			}

			blockIndicesStream <- uint64(blockIndex)
		}
	}

	//
	wg := readUnit(db, fromBlock, toBlock, queryFunc)

	go func() {
		wg.Wait()
		close(blockIndicesStream)
		close(errorStream)
	}()

	return blockIndicesStream, errorStream, nil
}

func readCompleteTxs(db *sql.DB, fromBlock uint, toBlock uint) (<-chan web3.Hash, <-chan error, error) {

	errorStream := make(chan error)
	txHashStream := make(chan web3.Hash)

	queryFunc := func(from uint, to uint) {

		completedTxQuery := "SELECT block_index, tx_hash FROM tx WHERE block_index NOT IN ( "
		completedTxQuery += "SELECT block_index FROM tx WHERE scaned_receipt = false"
		completedTxQuery += ")"
		completedTxQuery += "AND block_index >= ? AND block_index <= ?"

		//rows, err := db.Query(completedBlockQuery)
		rows, err := db.Query(completedTxQuery, from, to)
		if err != nil {
			fmt.Println("[error][readCompleteTxs]", err)
			return
		}

		defer rows.Close()

		for rows.Next() {

			var blockIndex uint
			var txHashByte []byte
			err = rows.Scan(&blockIndex, &txHashByte)
			if err != nil {
				errorStream <- fmt.Errorf("[readCompleteTxs]%v", err)
				break
			}

			txHash := web3.BytesToHash(txHashByte)
			txHashStream <- txHash
		}
	}

	wg := readUnit(db, fromBlock, toBlock, queryFunc)

	go func() {
		wg.Wait()
		close(txHashStream)
		close(errorStream)
	}()

	return txHashStream, errorStream, nil
}

func readCompltedTxLogs(db *sql.DB, fromBlock uint, toBlock uint) (<-chan txLogKey, <-chan error, error) {

	errorStream := make(chan error)
	txlogStream := make(chan txLogKey)

	queryFunc := func(from uint, to uint) {

		completedTxLogQuery := "SELECT tx.block_index, log.tx_hash, log.log_index FROM tx_log log "
		completedTxLogQuery += "JOIN tx tx ON tx.tx_hash = log.tx_hash "
		completedTxLogQuery += "WHERE log.scaned_log = true "
		completedTxLogQuery += "AND tx.block_index >= ? AND tx.block_index <= ?"

		//rows, err := db.Query(completedBlockQuery)
		rows, err := db.Query(completedTxLogQuery, from, to)
		if err != nil {
			errorStream <- fmt.Errorf("[readCompleteTxs]%v", err)
			return
		}
		defer rows.Close()

		for rows.Next() {

			var blockIndex uint
			var txHashBytes []byte
			var logIndex uint64
			err := rows.Scan(&blockIndex, &txHashBytes, &logIndex)
			if err != nil {
				errorStream <- fmt.Errorf("[readCompltedTxLogs]%v", err)
				break
			}

			txlogStream <- txLogKey{
				txHash:   web3.BytesToHash(txHashBytes),
				logIndex: logIndex,
			}
		}
	}

	wg := readUnit(db, fromBlock, toBlock, queryFunc)

	go func() {
		wg.Wait()
		close(txlogStream)
		close(errorStream)
	}()

	return txlogStream, errorStream, nil
}

func readUncompletedTxs(db *sql.DB, fromBlock uint, toBlock uint) (<-chan *txUncompletedStateStreamData, <-chan error, error) {

	errorStream := make(chan error)
	uncompletedTxStream := make(chan *txUncompletedStateStreamData)

	queryFunc := func(from uint, to uint) {

		uncompletedTxQuery := "SELECT tx.block_index, tx.tx_hash, tx.log_count, log.log_index FROM tx tx "
		uncompletedTxQuery += "JOIN tx_log log ON tx.tx_hash = log.tx_hash "
		uncompletedTxQuery += "WHERE tx.scaned_receipt = false "
		uncompletedTxQuery += "AND log.scaned_log = true "
		uncompletedTxQuery += "AND tx.block_index >= ? AND tx.block_index <= ?"

		//rows, err := db.Query(completedBlockQuery)
		rows, err := db.Query(uncompletedTxQuery, from, to)
		if err != nil {
			errorStream <- fmt.Errorf("[readUncompletedTxs]%v", err)
			return
		}
		defer rows.Close()

		var streamData *txUncompletedStateStreamData

		var txHashBytes []byte = nil
		var txLogCount int = 0

		for rows.Next() {

			var blockIndex uint
			var logIndex uint64
			err = rows.Scan(&blockIndex, &txHashBytes, &txLogCount, &logIndex)
			if err != nil {
				errorStream <- fmt.Errorf("[readUncompletedTxs]%v", err)
				break
			}

			txHash := web3.BytesToHash(txHashBytes)

			// 첫 생성
			if streamData == nil {
				streamData = &txUncompletedStateStreamData{
					txHash: txHash,
					uncompletedState: &txUncompleteState{
						totalLogCount: txLogCount,
						logs:          make(map[uint64]bool),
					},
				}

				streamData.uncompletedState.logs[logIndex] = true
				continue
			}

			// hash 달라 졌다면..
			if streamData.txHash != txHash {
				uncompletedTxStream <- streamData

				streamData = &txUncompletedStateStreamData{
					txHash: txHash,
					uncompletedState: &txUncompleteState{
						totalLogCount: txLogCount,
						logs:          make(map[uint64]bool),
					},
				}

				streamData.uncompletedState.logs[logIndex] = true
				continue
			}

			// hash가 그대로라면
			streamData.uncompletedState.logs[logIndex] = true
		}

		// streamData가 남아 있다면..
		if streamData != nil {
			uncompletedTxStream <- streamData
		}
	}

	wg := readUnit(db, fromBlock, toBlock, queryFunc)

	go func() {
		wg.Wait()
		close(uncompletedTxStream)
		close(errorStream)
	}()

	return uncompletedTxStream, errorStream, nil

}

func wrtieNewBlock(db *sql.DB, newBlock *dbNewBlock) {

	if newBlock == nil {
		return
	}

	blockHash := string(newBlock.blockHash.Bytes())
	blockTimestamp := newBlock.timestamp.Format("2006-01-02 03:04:05.999")
	scaned := newBlock.txCount == 0

	insertSql := "REPLACE INTO block(block_index, block_hash, block_timestamp, transction_count, scaned_transaction) VALUES(?, ?, ?, ?, ?);"

	_, err := db.Exec(insertSql, newBlock.blockIndex, blockHash, blockTimestamp, newBlock.txCount, scaned)
	if err != nil {
		fmt.Println("[wrtieNewBlock] : ", err)
		return
	}

	if scaned {
		logger.BlockCompleteLog(newBlock.blockIndex)
	}
}

// Transaction 초기 상태 저장
func writeNewTx(db *sql.DB, blockCacheState *BlockCacheState, newTxReceiptContenxt *dbNewTxReceiptContent) bool {

	if newTxReceiptContenxt == nil {
		return false
	}

	blockIndex := newTxReceiptContenxt.blockIndex
	transaction := newTxReceiptContenxt.transaction
	if transaction == nil {
		return false
	}

	receipt := newTxReceiptContenxt.receipt
	if receipt == nil {
		return false
	}

	txHash := string(transaction.Hash.Bytes())
	fromAddr := string(transaction.From.Bytes())
	toAddr := string(web3.ZeroAddress.Bytes())
	if transaction.To != nil {
		toAddr = string(transaction.To.Bytes())
	}

	ethValue := transaction.Value.Uint64()
	logCount := len(receipt.Logs)
	scaned := logCount == 0

	// Transaction receipt 상태 저장
	insertTxSql := "REPLACE INTO tx(block_index, tx_hash, from_address, to_address, eth_value, log_count, scaned_receipt) VALUE(?, ?, ?, ?, ?, ?, ?)"
	_, err := db.Exec(insertTxSql, blockIndex, txHash, fromAddr, toAddr, ethValue, logCount, scaned)
	if err != nil {
		fmt.Println("[error][writeNewTx] ", err)
		return false
	}

	// Receipt LogCount가 0이면 scan 된걸로 간주 한다.
	if scaned {
		logger.TxCompleteLog(&transaction.Hash)
		return true
	}

	// Transaction reciept 저장
	writeNewTxRceipt(db, receipt, blockCacheState)

	// Transaction Reciept 전체 내용 저장 확인
	isTxLogsConfirm, err := confirmTxLog(db, receipt)
	if err != nil {
		fmt.Println("[error][confirmTxLog] : ", err)
		return false
	}

	isTxConfirm := false
	if isTxLogsConfirm {

		logger.TxLogCompleteLog(&receipt.TransactionHash)

		// Tranaction Tx 전체 내용 저장 확인
		isTxConfirm, err = confirmTx(db, &transaction.Hash)
		if err != nil {
			fmt.Println("[error][confirmTx] : ", err)
			return false
		}
	}

	return isTxConfirm
}

// Transaction Receiept	내용 저장
func writeNewTxRceipt(db *sql.DB, receipt *web3.Receipt, blockCacheState *BlockCacheState) {

	tx, err := db.Begin()
	if err != nil {
		return
	}

	isComplete := false
	defer func(isComplete *bool) {
		if *isComplete {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}(&isComplete)

	// scaned 된 Log 추리기
	completedLogIndices := blockCacheState.GetCompletedTxLogs(receipt.TransactionHash, receipt.Logs)

	// completedLog 갯수와 receiptLog 갯수가 같다면 pass
	if len(receipt.Logs) == len(completedLogIndices) {
		isComplete = true
		return
	}

	// log 상태 저장
	err = writeNewTxLog(tx, receipt.Logs, completedLogIndices)
	if err != nil {
		fmt.Println("[error][writeNewTxLog] : ", err)
		return
	}

	// log topic 저장
	err = writeNewTxLogTopic(tx, receipt.Logs, completedLogIndices)
	if err != nil {
		fmt.Println("[error][writeNewTxLogTopic] : ", err)
		return
	}

	isComplete = true
}

func writeNewTxLog(tx *sql.Tx, logs []*web3.Log, completedLogIndices map[uint64]bool) error {

	insertSql := "REPLACE INTO tx_log(tx_hash, log_index, log_address, topic_count, scaned_log) VALUE(?, ?, ?, ?, ?)"
	stmt, err := tx.Prepare(insertSql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, log := range logs {

		// 완료 된 log라면 패스...
		_, exists := completedLogIndices[log.LogIndex]
		if exists {
			continue
		}

		txHash := string(log.TransactionHash.Bytes())
		logAddress := string(log.Address.Bytes())
		topicCount := len(log.Topics)
		scaned := topicCount == 0

		_, err = stmt.Exec(txHash, log.LogIndex, logAddress, topicCount, scaned)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeNewTxLogTopic(tx *sql.Tx, logs []*web3.Log, completedLogIndices map[uint64]bool) error {

	insertSql := "REPLACE INTO tx_log_topic(tx_hash, log_index, topic_index, topic) VALUE(?, ?, ?, ?)"
	stmt, err := tx.Prepare(insertSql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, log := range logs {

		// 완료 된 log라면 패스...
		_, exists := completedLogIndices[log.LogIndex]
		if exists {
			continue
		}

		txHash := string(log.TransactionHash.Bytes())

		// topic이 없다면 무시..
		if len(log.Topics) == 0 {
			continue
		}

		for idx, topic := range log.Topics {

			topicValue := string(topic.Bytes())
			_, err = stmt.Exec(txHash, log.LogIndex, idx, topicValue)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func confirmBlock(db *sql.DB, blockIndex uint64) (bool, error) {

	blockTxCountQuery := "SELECT transction_count FROM block WHERE block_index = ?"
	rows, err := db.Query(blockTxCountQuery, blockIndex)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return false, fmt.Errorf("block_index = %v not found", blockIndex)
	}

	var blockTxCount uint
	err = rows.Scan(&blockTxCount)
	if err != nil {
		return false, err
	}

	recordBlockTxCountQuery := "SELECT COUNT(*) FROM tx WHERE scaned_receipt = true AND block_index = ?"
	row := db.QueryRow(recordBlockTxCountQuery, blockIndex)
	if row.Err() != nil {
		return false, row.Err()
	}

	var recordBlockCount uint
	err = row.Scan(&recordBlockCount)
	if err != nil {
		return false, err
	}

	//fmt.Printf("[blockindex = %v]blockTxCount = %v, recordBlockCount= %v\n", blockIndex, blockTxCount, recordBlockCount)

	// 기록 된 Transaction 갯수가 다르다.
	if blockTxCount != recordBlockCount {
		return false, nil
	}

	updateSql := "UPDATE block SET scaned_transaction = true WHERE block_index = ?"
	_, err = db.Exec(updateSql, blockIndex)
	if err != nil {
		return false, err
	}

	return true, nil
}

func confirmTx(db *sql.DB, transactionHash *web3.Hash) (bool, error) {

	txHash := string(transactionHash.Bytes())

	// tx 테이블에 log_count 가져오기
	txlogCountQuery := "SELECT log_count FROM tx WHERE tx_hash = ?"
	rows, err := db.Query(txlogCountQuery, txHash)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return false, fmt.Errorf("tx_hash : \"%v\" not fount", *transactionHash)
	}

	var txLogCount uint
	err = rows.Scan(&txLogCount)
	if err != nil {
		return false, err
	}

	// recording 된 txLogCount 가져오기
	logCountQuery := "SELECT count(DISTINCT log_index) FROM tx_log WHERE tx_hash = ? and scaned_log = true"
	row := db.QueryRow(logCountQuery, txHash)
	if row.Err() != nil {
		return false, err
	}

	var recordTxLogCount uint
	err = row.Scan(&recordTxLogCount)
	if err != nil {
		return false, err
	}

	// transaction의 log가 모두 저장이 안되었다..
	if txLogCount != recordTxLogCount {
		return false, nil
	}

	updateSql := "UPDATE tx SET scaned_receipt = true WHERE tx_hash = ?"
	_, err = db.Exec(updateSql, txHash)
	if err != nil {
		return false, err
	}

	return true, nil
}

func confirmTxLog(db *sql.DB, receipt *web3.Receipt) (bool, error) {

	txHash := string(receipt.TransactionHash.Bytes())

	logTopicCountCacheMap := make(map[uint]uint)
	recordLogTopicCountMap := make(map[uint]uint)

	// tx_log 초기 상태 가져오기
	err := func() error {

		txLogQuery := "SELECT log_index, topic_count FROM tx_log WHERE tx_hash = ? "
		rows, err := db.Query(txLogQuery, txHash)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {

			var logIndex uint
			var topicCount uint

			err = rows.Scan(&logIndex, &topicCount)
			if err != nil {
				return err
			}

			logTopicCountCacheMap[logIndex] = topicCount
		}

		return nil
	}()

	if err != nil {
		return false, err
	}

	// 실제 저장 된 tx_log 관련 정보 가져오기
	err = func() error {

		recordLogTopicCountQuery := "SELECT log_index, count(DISTINCT topic_index) FROM tx_log_topic WHERE tx_hash = ? GROUP BY log_index"
		rows, err := db.Query(recordLogTopicCountQuery, txHash)
		if err != nil {
			return err
		}

		for rows.Next() {

			var logIndex uint
			var topicCount uint

			err = rows.Scan(&logIndex, &topicCount)
			if err != nil {
				return err
			}

			recordLogTopicCountMap[logIndex] = topicCount
		}

		return nil
	}()

	if err != nil {
		return false, err
	}

	completeLogIndices := make([]uint, 0)
	// 초기 txLog 샅애 기준으로 확인
	for logIndex, topicCount := range logTopicCountCacheMap {

		recordTopicCount, exists := recordLogTopicCountMap[logIndex]
		if exists == false {
			fmt.Printf("[confirmTxLog] logIndex Not Found [tx : %v, LogIndex : %v]\n", receipt.TransactionHash, logIndex)
			continue
		}

		if topicCount == recordTopicCount {
			completeLogIndices = append(completeLogIndices, logIndex)
		}
	}

	// 정상적으로 확인 된 tx_log 체크 하기
	err = func() error {
		updateSql := "UPDATE tx_log SET scaned_log = true WHERE tx_hash = ? and log_index = ?"
		stmt, err := db.Prepare(updateSql)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, logIndex := range completeLogIndices {
			_, err := stmt.Exec(txHash, logIndex)
			if err != nil {
				fmt.Printf("[confirmTxLog] %v\n", err)
				continue
			}
		}

		return nil
	}()

	if err != nil {
		return false, err
	}

	return true, nil
}
