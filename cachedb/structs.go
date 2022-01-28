package cachedb

import (
	"time"

	"github.com/umbracle/go-web3"
)

type DbBlock struct {
	BlockNumber    uint64
	BlockHash      web3.Hash
	BlockTimeStamp time.Time

	Transactions []DbTransaction
}

type DbTransaction struct {
	BlockHash       web3.Hash
	TransactionHash web3.Hash
	FromAddress     web3.Address
	ToAddress       web3.Address
	Value           uint64
}

type DbTransactionLog struct {
	TransactionHash web3.Hash
	LogIndex        uint32
	LogAddress      web3.Address
	Topics          []web3.Hash
}
