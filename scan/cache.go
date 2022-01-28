package scan

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/umbracle/go-web3"
)

type txLogKey struct {
	txHash   web3.Hash
	logIndex uint64
}

type blockUncompleteState struct {
	totalTxsCount int
	txs           map[web3.Hash]bool
}

type txUncompleteState struct {
	totalLogCount int
	logs          map[uint64]bool // scaned_log = true인 log들
}

type txUncompletedStateStreamData struct {
	txHash           web3.Hash
	uncompletedState *txUncompleteState
}

type BlockCacheState struct {
	// scaned true인 block 리스트
	completedBlockIndices      map[uint64]bool
	completedBlockIndicesMutex sync.Mutex

	// scaned true인 tx 리스트
	completedTxHashs      map[web3.Hash]bool
	completedTxHashsMutex sync.Mutex

	// scaned true인 txLog 리스트
	completedTxLogs      map[txLogKey]bool
	completedTxLogsMutex sync.Mutex

	// 초기 상태는 기록 되었지만 완료 안된 Block 리스트
	uncompletedBlockState      map[uint64]*blockUncompleteState
	uncompletedBlockStateMutex sync.Mutex

	// 초기 상태는 기록 되었지만 완료 안된 Tx 리스트
	uncompletedTxState      map[web3.Hash]*txUncompleteState
	uncompletedTxStateMutex sync.Mutex
}

type BlockConfirm uint64
type TxConform struct {
	blockIndex uint64
	txHash     web3.Hash
}

func (b *BlockCacheState) GetCompletedBlockCount() int {
	return len(b.completedBlockIndices)
}

func (b *BlockCacheState) GetComletedTxCount() int {
	return len(b.completedTxHashs)
}

func (b *BlockCacheState) GetComletedTxLogCount() int {
	return len(b.completedTxLogs)
}

func (b *BlockCacheState) GetUncompletedBlockCount() int {
	return len(b.uncompletedBlockState)
}

func (b *BlockCacheState) RegisterUncompletedBlock(block *web3.Block) {
	b.uncompletedBlockStateMutex.Lock()
	defer b.uncompletedBlockStateMutex.Unlock()

	b.uncompletedBlockState[block.Number] = &blockUncompleteState{
		txs:           make(map[web3.Hash]bool),
		totalTxsCount: len(block.Transactions),
	}
}

/// result : true  [cache에 있는 block tx 체크 되었다]
func (b *BlockCacheState) CheckUncompletedBlock(blockIndex uint64, tx *web3.Hash) bool {

	b.uncompletedBlockStateMutex.Lock()
	defer b.uncompletedBlockStateMutex.Unlock()

	state, exists := b.uncompletedBlockState[blockIndex]
	if !exists {
		return false
	}

	state.txs[*tx] = true

	txsLen := len(state.txs)
	maxTxsLen := state.totalTxsCount

	return maxTxsLen == txsLen
}

func (b *BlockCacheState) CompleteUnCompletedBlock(blockIndex uint64) {
	b.uncompletedBlockStateMutex.Lock()
	defer b.uncompletedBlockStateMutex.Unlock()

	delete(b.uncompletedBlockState, blockIndex)
}

func (b *BlockCacheState) IsCompletedBlock(blockIndex uint64) bool {
	b.completedBlockIndicesMutex.Lock()
	defer b.completedBlockIndicesMutex.Unlock()

	_, exists := b.completedBlockIndices[blockIndex]
	if exists {
		delete(b.completedBlockIndices, blockIndex)
	}

	return exists
}

func (b *BlockCacheState) IsCompletedTx(txHash web3.Hash) bool {
	b.completedTxHashsMutex.Lock()
	defer b.completedTxHashsMutex.Unlock()

	_, exists := b.completedTxHashs[txHash]
	if exists {
		delete(b.completedTxHashs, txHash)
	}

	return exists
}

func (b *BlockCacheState) GetCompletedTxLogs(txHash web3.Hash, logs []*web3.Log) map[uint64]bool {
	b.completedTxLogsMutex.Lock()
	defer b.completedTxLogsMutex.Unlock()

	completedLogIndices := make(map[uint64]bool)

	for _, log := range logs {
		key := txLogKey{txHash: txHash, logIndex: log.LogIndex}

		_, exists := b.completedTxLogs[key]
		if !exists {
			continue
		}

		delete(b.completedTxLogs, key)
		completedLogIndices[log.LogIndex] = true
	}

	return completedLogIndices
}

func (b *BlockCacheState) IsCompletedTxLog(txHash web3.Hash, logIndex uint64) bool {
	b.completedTxLogsMutex.Lock()
	defer b.completedTxLogsMutex.Unlock()

	key := txLogKey{txHash: txHash, logIndex: logIndex}

	_, exists := b.completedTxLogs[key]
	if exists {
		delete(b.completedTxLogs, key)
	}

	return exists
}

func (b *BlockCacheState) IsUncompletedBlock(blockIndex uint64) bool {
	b.uncompletedBlockStateMutex.Lock()
	defer b.uncompletedBlockStateMutex.Unlock()

	_, exists := b.uncompletedBlockState[blockIndex]
	if exists {
		delete(b.uncompletedBlockState, blockIndex)
	}

	return exists
}

// db 상에서는 완료가 되어야 하는 상태
func (b *BlockCacheState) IsCompleteInCacheTx(txHash web3.Hash) bool {
	//b.uncompletedTxStateMutex.Lock()
	//defer b.uncompletedTxStateMutex.Unlock()

	txState, exists := b.uncompletedTxState[txHash]
	if !exists {
		return false
	}

	completedTx := len(txState.logs) == txState.totalLogCount
	return completedTx
}

func NewBlockCacheState(dbConnectionString string, fromBlock uint, toBlock uint) (*BlockCacheState, error) {

	blockCacheState := &BlockCacheState{
		completedBlockIndices:      make(map[uint64]bool),
		completedBlockIndicesMutex: sync.Mutex{},

		completedTxHashs:      make(map[web3.Hash]bool),
		completedTxHashsMutex: sync.Mutex{},

		completedTxLogs:      make(map[txLogKey]bool),
		completedTxLogsMutex: sync.Mutex{},

		// 초기 상태는 기록 되었지만 완료 안된 Block 리스트
		uncompletedBlockState:      make(map[uint64]*blockUncompleteState),
		uncompletedBlockStateMutex: sync.Mutex{},

		// 초기 상태는 기록 되었지만 완료 안된 Tx 리스트
		uncompletedTxState:      make(map[web3.Hash]*txUncompleteState),
		uncompletedTxStateMutex: sync.Mutex{},
	}

	db, err := sql.Open("mysql", dbConnectionString)
	if err != nil {
		return nil, err
	}

	blockStream, blockErrorStream, err := readCompletedBlocks(db, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	txStream, txErrorStream, err := readCompleteTxs(db, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	txLogStream, txLogErrorStream, err := readCompltedTxLogs(db, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	uncompleteTxStream, uncompleteTxErrorStream, err := readUncompletedTxs(db, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for blockIndex := range blockStream {
			blockCacheState.completedBlockIndices[blockIndex] = true
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for txHash := range txStream {
			blockCacheState.completedTxHashs[txHash] = true
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for txLogKey := range txLogStream {
			blockCacheState.completedTxLogs[txLogKey] = true
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for uncompleteTx := range uncompleteTxStream {
			blockCacheState.uncompletedTxState[uncompleteTx.txHash] = uncompleteTx.uncompletedState
		}
	}()

	// error 체크
	errors := make([]error, 0)
	wg.Add(1)
	go func() {
		defer wg.Done()

		errorCatch := func(errorStream <-chan error) {
			for err := range errorStream {
				errors = append(errors, err)
			}
		}

		errorCatch(blockErrorStream)
		errorCatch(txErrorStream)
		errorCatch(txLogErrorStream)
		errorCatch(uncompleteTxErrorStream)

	}()
	wg.Wait()

	var dbError error = nil
	if len(errors) > 0 {
		dbError = fmt.Errorf("occur error block cache read")
	}

	return blockCacheState, dbError
}
