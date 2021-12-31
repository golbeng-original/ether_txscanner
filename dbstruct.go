package main

import (
	"time"

	"github.com/umbracle/go-web3"
)

type DbBlock struct {
	BlockNumber    uint64
	BlockHash      web3.Hash
	BlockTimeStamp time.Time
}

type DbBlockTx struct {
	BlockHash       web3.Hash
	TransactionHash web3.Hash

	FromAddr web3.Address
	ToAddr   web3.Address
	Value    uint64
}

type DbTxLog struct {
	TransactionHash web3.Hash
	LogIndex        int
}

type DbErc721TransferTopic struct {
	TransactionHash web3.Hash
	LogIndex        uint64
	TopicIndex      uint64

	From    web3.Address
	To      web3.Address
	TokenId web3.Address
}
