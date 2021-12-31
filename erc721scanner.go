package main

import (
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/umbracle/go-web3"
	"github.com/umbracle/go-web3/abi"
	"github.com/umbracle/go-web3/jsonrpc"
)

type Erc721TransferEvent struct {
	TransactionHash web3.Hash
	TimeStamp       time.Time
	ContractAddress web3.Address
	FromAddress     web3.Address
	ToAddress       web3.Address
	TokenId         uint64
}

type Erc721TransactionScanner struct {
	Transactions []Tranaction
}

var transferEvent = abi.MustNewEvent("Transfer(address indexed from, address indexed to, uint256 indexed tokenId)")

/// Transaction으로부터 ERC721 Transfer event를 추출한다.
func (scanner *Erc721TransactionScanner) ScanErc721TransferEvent(client *jsonrpc.Client) []Erc721TransferEvent {

	erc721TransferList := make([]Erc721TransferEvent, 0)

	for _, tx := range scanner.Transactions {

		result := findErc721TransferEvent_new(&tx)

		erc721TransferList = append(erc721TransferList, result...)
	}

	sort.Slice(erc721TransferList, func(i, j int) bool {
		return erc721TransferList[i].TimeStamp.After(erc721TransferList[j].TimeStamp)
	})

	for idx, event := range erc721TransferList {
		fmt.Println(idx, " - ", event)
	}

	return erc721TransferList
}

func findErc721TransferEvent_new(tx *Tranaction) []Erc721TransferEvent {

	erc721TransferEvents := make([]Erc721TransferEvent, 0)

	for _, log := range tx.Receipt.Logs {

		// sign, address, address, tokenId
		if len(log.Topics) != 4 {
			continue
		}
		isMatch := transferEvent.Match(log)
		if !isMatch {
			continue
		}

		parsed, err := transferEvent.ParseLog(log)
		if err != nil {
			fmt.Printf("sign = %s, err = %v\n", log.Topics[0].String(), err)
			continue
		}

		erc721TransactionEvent := Erc721TransferEvent{
			TransactionHash: tx.Hash,
			ContractAddress: log.Address,
			TimeStamp:       tx.TimeStamp,
			FromAddress:     parsed["from"].(web3.Address),
			ToAddress:       parsed["to"].(web3.Address),
			TokenId:         parsed["tokenId"].(*big.Int).Uint64(),
		}

		erc721TransferEvents = append(erc721TransferEvents, erc721TransactionEvent)
	}

	return erc721TransferEvents
}
