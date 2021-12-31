package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/umbracle/go-web3"
	"github.com/umbracle/go-web3/jsonrpc"
)

type TransactionScanner struct {
	ApiKey         string
	AccountAddress string
}

func (scanner *TransactionScanner) RequestTxs(client *jsonrpc.Client) ([]Tranaction, error) {

	latestBlockNumber, err := client.Eth().BlockNumber()
	if err != nil {
		return nil, err
	}

	etherscanApi := "https://api.etherscan.io/api?"
	etherscanApi += "module=account&action=txlist&"
	etherscanApi += fmt.Sprintf("address=%s&", scanner.AccountAddress)
	etherscanApi += fmt.Sprintf("startblock=0&endblock=%d&", latestBlockNumber)
	etherscanApi += "page=1&offset=10000&sort=desc&"
	etherscanApi += fmt.Sprintf("apikey=%s", scanner.ApiKey)

	response, err := http.Get(etherscanApi)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	bodyContent, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	txList, err := scanner.parseTxContent(bodyContent)
	if err != nil {
		return nil, err
	}

	return collectTxReceipt(client, txList), nil
}

func (scanner *TransactionScanner) parseTxContent(content []byte) ([]accountTx, error) {

	var accountTxList accountTxList
	err := json.Unmarshal(content, &accountTxList)
	if err != nil {
		return nil, err
	}

	return accountTxList.Result, nil
}

func collectTxReceipt(client *jsonrpc.Client, txs []accountTx) []Tranaction {

	transactions := make([]Tranaction, 0)

	done := make(chan bool)
	ch := make(chan *Tranaction)

	go func() {
		for tranaction := range ch {
			transactions = append(transactions, *tranaction)
		}

		done <- true
	}()

	// reciept 조사
	wg := sync.WaitGroup{}
	for idx := range txs {
		wg.Add(1)

		go func(tx *accountTx) {
			defer wg.Done()

			receipt := requestReceipt(client, tx)
			if receipt == nil {
				return
			}

			transaction := createTransaction(tx)
			transaction.Receipt = receipt

			ch <- transaction

		}(&txs[idx])
	}

	wg.Wait()
	close(ch)

	<-done

	return transactions
}

func requestReceipt(client *jsonrpc.Client, tx *accountTx) *web3.Receipt {

	hash := web3.HexToHash(tx.Hash)

	receipt, err := client.Eth().GetTransactionReceipt(hash)
	if err != nil {
		return nil
	}

	return receipt
}
