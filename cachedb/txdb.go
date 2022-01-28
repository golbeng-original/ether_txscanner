package cachedb

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/umbracle/go-web3"
)

type BlockDatabase struct {
	ConnectString string
	connection    *sql.DB
	maxConnection int
}

func NewDatabase(connString string) (*BlockDatabase, error) {

	database := BlockDatabase{
		ConnectString: connString,
	}

	err := database.open()
	if err != nil {
		return nil, err
	}

	return &database, nil
}

func (blockDb *BlockDatabase) open() error {

	var err error
	blockDb.connection, err = sql.Open("mysql", blockDb.ConnectString)
	if err != nil {
		return err
	}

	return nil
}

func (blockDB *BlockDatabase) SetMaxOpenConns(connCount int) {

	blockDB.maxConnection = connCount

	blockDB.connection.SetMaxIdleConns(connCount)
	blockDB.connection.SetMaxOpenConns(connCount)
}

func (blockDB *BlockDatabase) Close() {
	if blockDB.connection == nil {
		return
	}

	blockDB.connection.Close()
}

func (blockDb *BlockDatabase) GetRecordBlockCacheAsync() <-chan DbBlock {

	queryTransactionStream := make(chan DbBlock)
	blockCacheStream := make(chan DbBlock)

	go func() {
		defer close(queryTransactionStream)

		query := "SELECT block_num, block_hash, block_timestamp FROM block"

		rows, err := blockDb.connection.Query(query)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer rows.Close()

		for rows.Next() {

			var blockIndex uint64
			var blockHash []byte
			var blockTimeStampRaw []byte

			err = rows.Scan(&blockIndex, &blockHash, &blockTimeStampRaw)
			if err != nil {
				continue
			}

			blockTimeStamp, err := time.Parse("2006-01-02 15:04:05.999", string(blockTimeStampRaw))
			if err != nil {
				blockTimeStamp = time.Unix(0, 0).UTC()
			}

			block := DbBlock{
				BlockNumber:    blockIndex,
				BlockHash:      web3.BytesToHash(blockHash),
				BlockTimeStamp: blockTimeStamp,
			}

			queryTransactionStream <- block
		}
	}()

	go func() {
		defer close(blockCacheStream)

		// Transaction 정보 가져오기
		for blockCache := range queryTransactionStream {
			dbTransactions := blockDb.getRecordTransactionCacheFromBlock(blockCache)
			blockCache.Transactions = dbTransactions

			blockCacheStream <- blockCache
		}
	}()

	return blockCacheStream
}

func (blockDb *BlockDatabase) GetRecordBlockCache() []DbBlock {

	query := "SELECT block_num, block_hash, block_timestamp FROM block"

	rows, err := blockDb.connection.Query(query)
	if err != nil {
		return nil
	}

	defer rows.Close()

	dbBlocks := make([]DbBlock, 0)

	for rows.Next() {

		var blockIndex uint64
		var blockHash []byte
		var blockTimeStampRaw []byte

		err = rows.Scan(&blockIndex, &blockHash, &blockTimeStampRaw)
		if err != nil {
			continue
		}

		blockTimeStamp, err := time.Parse("2006-01-02 15:04:05.999", string(blockTimeStampRaw))
		if err != nil {
			blockTimeStamp = time.Unix(0, 0).UTC()
		}

		block := DbBlock{
			BlockNumber:    blockIndex,
			BlockHash:      web3.BytesToHash(blockHash),
			BlockTimeStamp: blockTimeStamp,
		}

		dbBlocks = append(dbBlocks, block)
	}

	// Transaction 정보 가져오기
	txSet := blockDb.getRecordTransactionCache()
	if txSet != nil {

		for blockIdx := range dbBlocks {

			blockHash := dbBlocks[blockIdx].BlockHash
			txs, exist := (*txSet)[blockHash]
			if !exist {
				continue
			}

			dbBlocks[blockIdx].Transactions = txs
		}
	}

	return dbBlocks
}

func newDbTransactionLog(transactionHash web3.Hash, logIndex uint32, logAddress web3.Address) *DbTransactionLog {
	return &DbTransactionLog{
		TransactionHash: transactionHash,
		LogIndex:        logIndex,
		LogAddress:      logAddress,
		Topics:          make([]web3.Hash, 0),
	}
}

func (blockDb *BlockDatabase) GetRecordTransactionLogCache() []DbTransactionLog {

	query := "SELECT tx_hash, log_index, log_address, topic FROM tx_log order by tx_hash"
	rows, err := blockDb.connection.Query(query)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	defer rows.Close()

	var dbTransactionLog *DbTransactionLog

	dbTrnasactionLogs := make([]DbTransactionLog, 0)
	for rows.Next() {
		var transactionHashByte []byte
		var logIndex uint32
		var logAddressByte []byte
		var topicByte []byte

		err = rows.Scan(&transactionHashByte, &logIndex, &logAddressByte, &topicByte)
		if err != nil {
			fmt.Println(err)
			continue
		}

		transactionHash := web3.BytesToHash(transactionHashByte)
		logAddress := web3.BytesToAddress(logAddressByte)

		if dbTransactionLog == nil {
			dbTransactionLog = newDbTransactionLog(transactionHash, logIndex, logAddress)
			dbTransactionLog.Topics = append(dbTransactionLog.Topics, web3.BytesToHash(topicByte))
		} else if dbTransactionLog.TransactionHash == transactionHash &&
			dbTransactionLog.LogAddress == logAddress {
			dbTransactionLog.Topics = append(dbTransactionLog.Topics, web3.BytesToHash(topicByte))
		} else {
			dbTrnasactionLogs = append(dbTrnasactionLogs, *dbTransactionLog)

			dbTransactionLog = newDbTransactionLog(transactionHash, logIndex, logAddress)
			dbTransactionLog.Topics = append(dbTransactionLog.Topics, web3.BytesToHash(topicByte))
		}
	}

	if dbTransactionLog != nil {
		dbTrnasactionLogs = append(dbTrnasactionLogs, *dbTransactionLog)
	}

	return dbTrnasactionLogs
}

func (blockDb *BlockDatabase) GetRecordScanedReceiptTxHash() []web3.Hash {

	sql := "SELECT tx_hash FROM tx WHERE scaned_receipt = true"

	rows, err := blockDb.connection.Query(sql)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	defer rows.Close()

	scanedTxs := make([]web3.Hash, 0)
	for rows.Next() {

		var txHashByte []byte

		err = rows.Scan(&txHashByte)
		if err != nil {
			continue
		}

		scanedTxs = append(scanedTxs, web3.BytesToHash(txHashByte))
	}

	return scanedTxs
}

func (blockDb *BlockDatabase) getRecordTransactionCacheFromBlock(block DbBlock) []DbTransaction {

	query := "SELECT tx_hash, block_hash, from_address, to_address, eth_value FROM tx WHERE block_hash = ?"

	rows, err := blockDb.connection.Query(query, block.BlockHash.String())
	if err != nil {
		fmt.Println("db error ", err)
		return nil
	}

	defer rows.Close()

	transactions := make([]DbTransaction, 0)

	for rows.Next() {
		var blockHashByte []byte
		var txHashByte []byte
		var fromAddrByte []byte
		var toAddrByte []byte
		var value uint64

		err = rows.Scan(&txHashByte, &blockHashByte, &fromAddrByte, &toAddrByte, &value)
		if err != nil {
			fmt.Println(err)
			continue
		}

		transactions = append(transactions, DbTransaction{
			BlockHash:       block.BlockHash,
			TransactionHash: web3.BytesToHash(txHashByte),
			FromAddress:     web3.BytesToAddress(fromAddrByte),
			ToAddress:       web3.BytesToAddress(toAddrByte),
			Value:           value,
		})
	}

	return transactions
}

func (blockDb *BlockDatabase) getRecordTransactionCache() *map[web3.Hash][]DbTransaction {

	query := "SELECT tx_hash, block_hash, from_address, to_address, eth_value FROM tx" // WHERE block_hash = ?"

	rows, err := blockDb.connection.Query(query) //, block.BlockHash.String())
	if err != nil {
		fmt.Println("db error ", err)
		return nil
	}

	defer rows.Close()

	txSet := make(map[web3.Hash][]DbTransaction)

	for rows.Next() {

		var blockHashByte []byte
		var txHashByte []byte
		var fromAddrByte []byte
		var toAddrByte []byte
		var value uint64

		err = rows.Scan(&txHashByte, &blockHashByte, &fromAddrByte, &toAddrByte, &value)
		if err != nil {
			fmt.Println(err)
			continue
		}

		blockHash := web3.BytesToHash(blockHashByte)

		_, exist := txSet[blockHash]
		if !exist {
			txSet[blockHash] = make([]DbTransaction, 0)
		}

		txSet[blockHash] = append(txSet[blockHash], DbTransaction{
			BlockHash:       blockHash,
			TransactionHash: web3.BytesToHash(txHashByte),
			FromAddress:     web3.BytesToAddress(fromAddrByte),
			ToAddress:       web3.BytesToAddress(toAddrByte),
			Value:           value,
		})
	}

	return &txSet
}

func (blockDb *BlockDatabase) ExistBlockFromIndex(blockIndex uint64) bool {

	query := "SELECT count(*) FROM block WHERE block_num = ?;"
	row := blockDb.connection.QueryRow(query, blockIndex)

	if row.Err() != nil {
		fmt.Println("db error : ", row.Err())
		return false
	}

	var count int
	err := row.Scan(&count)
	if err != nil {
		//fmt.Println("db Error : ", err)
		return false
	}

	if count == 0 {
		return false
	}

	return true
}

func (blockDb *BlockDatabase) WriteBlock(block *DbBlock) {

	blockHash := string(block.BlockHash.Bytes())
	blockIndex := block.BlockNumber
	blocktimeStamp := block.BlockTimeStamp.Format("2006-01-02 03:04:05.999")

	insertQuery := "INSERT INTO block(block_hash, block_num, block_timestamp) VALUES(?, ?, ?);"

	//insertQuery = fmt.Sprintf(insertQuery, blockHash, blockIndex, blocktimeStamp)

	_, err := blockDb.connection.Exec(insertQuery, blockHash, blockIndex, blocktimeStamp)
	if err != nil {
		fmt.Println(err)
		blockDb.WriteErrorBlock(block.BlockNumber)
		return
	}

	// Transaction 저장
	err = blockDb.writeTransaction(block)
	if err != nil {
		fmt.Println(err)
		blockDb.WriteErrorBlock(block.BlockNumber)

	}
}

func (blockDb *BlockDatabase) MarkScanedReceipt(txHash web3.Hash) {

	txHashStr := string(txHash.Bytes())

	query := "UPDATE tx SET scaned_receipt = TRUE WHERE tx_hash = ?"

	_, err := blockDb.connection.Exec(query, txHashStr)
	if err != nil {
		fmt.Println(err)
	}
}

func (blockDb *BlockDatabase) writeTransaction(block *DbBlock) error {

	if len(block.Transactions) == 0 {
		return nil
	}

	insertQuery := "INSERT INTO tx(tx_hash, block_hash, from_address, to_address, eth_value) VALUES(?, ?, ?, ?, ?);"
	stmt, err := blockDb.connection.Prepare(insertQuery)
	if err != nil {
		return err
	}

	defer stmt.Close()

	for _, transaction := range block.Transactions {

		blockHash := string(transaction.BlockHash.Bytes())
		txHash := string(transaction.TransactionHash.Bytes())
		fromAddr := string(transaction.FromAddress.Bytes())
		toAddr := string(transaction.ToAddress.Bytes())

		stmt.Exec(txHash, blockHash, fromAddr, toAddr, transaction.Value)
	}

	return nil
}

func (blockDb *BlockDatabase) WriteTransactionLog(txLog *DbTransactionLog) {

	insertQuery := "INSERT INTO tx_log(tx_hash, log_index, log_address, topic) VALUE(?, ?, ?, ?)"

	stmt, err := blockDb.connection.Prepare(insertQuery)
	if err != nil {
		fmt.Println(err)
		// 실패 log 기록
		blockDb.WriteErrorTransactionReceipt(txLog.TransactionHash)
		return
	}

	defer stmt.Close()

	txHash := string(txLog.TransactionHash.Bytes())
	logAddress := string(txLog.LogAddress.Bytes())

	for _, topic := range txLog.Topics {
		topicValue := string(topic.Bytes())
		stmt.Exec(txHash, txLog.LogIndex, logAddress, topicValue)
	}
}

func (blockDb *BlockDatabase) WriteErrorBlock(blockNumber uint64) {

	insertQuery := "INSERT INTO error_block_num(block_num) VALUES(?);"

	_, err := blockDb.connection.Exec(insertQuery, blockNumber)
	if err != nil {
		fmt.Println("[error]", err)
	}
}

func (blockDb *BlockDatabase) WriteErrorTransactionReceipt(txHash web3.Hash) {
	txHashStr := string(txHash.Bytes())
	insertQuery := "INSERT INTO error_tx(tx_hash) VALUES(?);"

	_, err := blockDb.connection.Exec(insertQuery, txHashStr)
	if err != nil {
		fmt.Println("[error]", err)
	}
}
