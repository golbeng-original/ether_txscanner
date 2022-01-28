package logger

import (
	"fmt"

	"github.com/umbracle/go-web3"
)

var EnableCatchedLog = true

func CatchedBlockCompleteLog(blockIndex uint64) {
	if !EnableCatchedLog {
		return
	}

	fmt.Printf("[Block] %v reocrd complete [cached]\n", blockIndex)
}

func BlockCompleteLog(blockIndex uint64) {
	fmt.Printf("[Block] %v reocrd complete\n", blockIndex)
}

func CatchedTxCompleteLog(txHash *web3.Hash) {
	if !EnableCatchedLog {
		return
	}

	fmt.Printf("[Transaction] %v reocrd complete [cached]\n", *txHash)
}

func TxCompleteLog(txHash *web3.Hash) {
	fmt.Printf("[Transaction] %v reocrd complete\n", *txHash)
}

func TxLogCompleteLog(txHash *web3.Hash) {
	fmt.Printf("[Transaction Logs] %v reocrd complete\n", *txHash)
}
