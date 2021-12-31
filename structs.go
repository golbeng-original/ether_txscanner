package main

import (
	"strconv"
	"time"

	"github.com/umbracle/go-web3"
)

type accountTxList struct {
	Status string      `json:"status"`
	Result []accountTx `json:"result"`
}

type accountTx struct {
	BlockNumber string `json:"blockNumber"`
	TimeStamp   string `json:"timeStamp"`
	Hash        string `json:"hash"`
	From        string `json:"from"`
	To          string `json:"to"`
	Value       string `json:"value"`
}

type Tranaction struct {
	BlockNumber uint64
	TimeStamp   time.Time
	Hash        web3.Hash
	From        web3.Address
	To          web3.Address
	Value       uint64

	Receipt *web3.Receipt
}

func createTransaction(accountTx *accountTx) *Tranaction {

	blockNumber, _ := strconv.ParseUint(accountTx.BlockNumber, 0, 64)
	timeStamp, _ := strconv.ParseInt(accountTx.TimeStamp, 0, 64)
	value, _ := strconv.ParseUint(accountTx.Value, 0, 64)

	tx := &Tranaction{
		BlockNumber: blockNumber,
		Hash:        web3.HexToHash(accountTx.Hash),
		TimeStamp:   time.Unix(timeStamp, 0).UTC(),
		From:        web3.HexToAddress(accountTx.From),
		To:          web3.HexToAddress(accountTx.To),
		Value:       value,
	}

	return tx
}
