package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/umbracle/go-web3"
	"github.com/umbracle/go-web3/jsonrpc"

	_ "github.com/go-sql-driver/mysql"
)

func erc721Scane() {

	client, err := jsonrpc.NewClient("https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer client.Close()

	//targetAddress := "0xF6Aafb3689D54DCA004BD7dF3926e88346B687D3"
	targetAddress := "0x66A760a769E5d7Fa47dFC4e01dD21dB432640Efe"

	transactionScanner := TransactionScanner{
		AccountAddress: targetAddress,
		ApiKey:         "Z66CR65XMITC22DSQAXF4V44P4CABU9DXI",
	}

	transactions, err := transactionScanner.RequestTxs(client)
	if err != nil {
		fmt.Println(err)
		return
	}

	erc721Scanner := Erc721TransactionScanner{
		Transactions: transactions,
	}

	result := erc721Scanner.ScanErc721TransferEvent(client)

	// 원래 25
	// 현재 17
	fmt.Println(len(result))
}

func blockScan(client *jsonrpc.Client, fromBlock uint64, toBlock uint64, scanUnit uint64) ([]DbBlock, error) {

	dbBlockList := make([]DbBlock, 0)

	var startBlockNumber uint64 = fromBlock
	for startBlockNumber <= toBlock {

		endBlockNumber := startBlockNumber + scanUnit
		if endBlockNumber > toBlock {
			endBlockNumber = toBlock
		}

		result, err := blockScanRange(client, 0, scanUnit)
		if err != nil {
			return nil, err
		}

		dbBlockList = append(dbBlockList, result...)

		startBlockNumber = endBlockNumber + 1
	}

	return dbBlockList, nil

}

func blockScanRange(client *jsonrpc.Client, fromBlock uint64, toBlock uint64) ([]DbBlock, error) {

	dbBlockList := make([]DbBlock, 0)

	done := make(chan bool)
	ch := make(chan *DbBlock)

	// block append
	go func() {

		for dbBlock := range ch {
			dbBlockList = append(dbBlockList, *dbBlock)
		}

		done <- true

	}()

	// block 가져오기
	var getBlockFunc = func(blockIndex uint64, ch chan *DbBlock, wg *sync.WaitGroup) {

		defer wg.Done()

		var block *web3.Block
		var err error

		for {
			block, err = client.Eth().GetBlockByNumber(web3.BlockNumber(blockIndex), true)

			if err != nil && err.Error() == "no free connections available to host" {
				time.Sleep(time.Duration(300 * time.Millisecond))
				continue
			} else if err != nil && err.Error() == "dialing to the given TCP address timed out" {
				time.Sleep(time.Duration(300 * time.Millisecond))
				continue
			} else if err != nil {
				fmt.Println("client error : ", err)
				return
			}
			break
		}

		if block == nil {
			return
		}

		timeStamp := time.Unix(int64(block.Timestamp), 0).UTC()

		dbBlock := DbBlock{
			BlockNumber:    block.Number,
			BlockHash:      block.Hash,
			BlockTimeStamp: timeStamp,
		}

		ch <- &dbBlock
	}

	//
	wg := sync.WaitGroup{}
	for i := fromBlock; i <= toBlock; i++ {
		wg.Add(1)
		go getBlockFunc(i, ch, &wg)
	}

	wg.Wait()

	close(ch)
	<-done

	return dbBlockList, nil

	/*
		latest_block, err := client.Eth().BlockNumber()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("latest Block = %v\n", latest_block)
	*/

	/*
		startTime := time.Now()

		block, err := client.Eth().GetBlockByNumber(web3.BlockNumber(latest_block), true)
		//fmt.Println(block)

		fmt.Printf("block num = %v\n", block.Number)
		fmt.Printf("block hash = %v\n", block.Hash)
		fmt.Printf("tx length = %v \n", len(block.Transactions))

		tx := block.Transactions[0]

		fmt.Printf("tx hash = %v\n", tx.Hash)
		fmt.Printf("tx from = %v\n", tx.From)
		fmt.Printf("tx to = %v\n", tx.To)
		fmt.Printf("tx value = %v\n", tx.Value)

		receipt, err := client.Eth().GetTransactionReceipt(tx.Hash)
		fmt.Printf("receipt logs = %v\n", len(receipt.Logs))

		fmt.Println(time.Now().Sub(startTime))

		topics := make([]*web3.Hash, 0)
		topic := web3.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
		topics = append(topics, &topic)

		filter := &web3.LogFilter{
			Address: []web3.Address{web3.HexToAddress("0x66A760a769E5d7Fa47dFC4e01dD21dB432640Efe")},
			//Topics:  topics,
		}

		logs, err := client.Eth().GetLogs(filter)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(logs)
	*/

}

func logQuery() {

	etherscanApi := "https://api.etherscan.io/api?"
	//etherscanApi += "module=account&action=txlist&"
	etherscanApi += "module=logs&action=getLogs&"
	etherscanApi += fmt.Sprintf("address=%s&", "0x177ef8787ceb5d4596b6f011df08c86eb84380dc")
	etherscanApi += fmt.Sprintf("fromBlock=0&toBlock=%v&", "latest")
	etherscanApi += fmt.Sprintf("topic0=%v&", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	//etherscanApi += "page=1&offset=10000&sort=desc&"
	etherscanApi += fmt.Sprintf("apikey=%s", "Z66CR65XMITC22DSQAXF4V44P4CABU9DXI")

	response, err := http.Get(etherscanApi)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer response.Body.Close()

	bodyContent, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(string(bodyContent))
}

func main() {

	/*
		db, err := sql.Open("mysql", "root:1111@tcp(127.0.0.1:3306)/ether_tx")
		if err != nil {
			fmt.Println(err)
			return
		}

		defer db.Close()
	*/

	//erc721Scane()

	//var transferEvent = abi.MustNewEvent("Transfer(address indexed from, address indexed to, uint256 indexed tokenId)")
	//fmt.Println(transferEvent.ID().String())

	//logQuery()

	client, err := jsonrpc.NewClient("https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161")
	if err != nil {
		return
	}

	defer client.Close()

	//
	client.SetMaxConnsLimit(10000)

	latestBlockNumber, err := client.Eth().BlockNumber()
	if err != nil {
		return
	}

	fmt.Printf("block numnber = %v\n", latestBlockNumber)

	latestBlockNumber = 50000

	//dbBlockList := make([]DbBlock, 0)

	startTime := time.Now()

	/*
		var scanBlockUnit uint64 = 10000
		var startBlockNumber uint64 = 0
		for startBlockNumber <= latestBlockNumber {

			endBlockNumber := startBlockNumber + scanBlockUnit
			if endBlockNumber > latestBlockNumber {
				endBlockNumber = latestBlockNumber
			}

			result, err := blockScanRange(client, 0, 10000)
			if err != nil {
				fmt.Println(err)
				return
			}

			dbBlockList = append(dbBlockList, result...)

			startBlockNumber = endBlockNumber + 1
		}
	*/

	//
	dbBlockList, err := blockScan(client, 0, latestBlockNumber, 10000)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(len(dbBlockList))

	fmt.Printf("finish %v", time.Now().Sub(startTime))
}
