package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/briandowns/spinner"
	"github.com/urfave/cli/v2"

	"github.com/umbracle/go-web3"
	"github.com/umbracle/go-web3/jsonrpc"
	"github.com/vtok/ether_txscanner/scan"
	"github.com/vtok/ether_txscanner/scan/logger"

	_ "github.com/go-sql-driver/mysql"
)

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

func queryLatestBlockFunc(web3Provider string) error {

	s := spinner.New(spinner.CharSets[35], 100*time.Millisecond)
	s.Start()
	defer s.Stop()

	client, err := jsonrpc.NewClient(web3Provider)
	if err != nil {
		return err
	}

	defer client.Close()

	latestblockNum, err := client.Eth().BlockNumber()
	if err != nil {
		return err
	}

	fmt.Println("\rlatestBlockNumber : ", latestblockNum)

	return nil
}

func scanFunc(fromBlock uint64, toBlock uint64, configure scan.BlockScanConfigure) error {

	if configure.Err() != nil {
		return configure.Err()
	}

	logger.EnableCatchedLog = false

	blockScanWorker := 1
	if configure.BlockScanWorker > 0 {
		blockScanWorker = configure.BlockScanWorker
	}

	dbWriteWorker := 1
	if configure.DBWriteWorker > 0 {
		dbWriteWorker = configure.DBWriteWorker
	}

	dbConfirmWorker := 1
	if configure.DBConfirmWorker > 0 {
		dbConfirmWorker = configure.DBConfirmWorker
	}

	web3Recorder := scan.Web3Recorder{
		BlockScanWorker:    blockScanWorker, //1024,
		DbWriteWorker:      dbWriteWorker,   //256, //740,
		DbConfirmWorker:    dbConfirmWorker, //64,  // 120
		BlockWeb3Providers: configure.Web3Providers,
		DBConnectionString: configure.DbConnString,
	}

	fmt.Println("Start Scaning")

	startTime := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := web3Recorder.BlockScan(ctx, fromBlock, toBlock)
	if err != nil {
		return err
	}

	fmt.Printf("Complete Scaning [%v]\n", time.Since(startTime))
	return nil
}

func makeLatestBlockCommand() *cli.Command {
	return &cli.Command{
		Name:    "get-latestblock",
		Aliases: []string{"gl"},
		Usage:   "specific net block scan and record",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "web3",
				Usage:    "web3 provider http/https url",
				Aliases:  []string{"w"},
				Required: true,
			},
		},
		Action: func(c *cli.Context) error {
			web3Provider := c.String("web3")

			return queryLatestBlockFunc(web3Provider)
		},
	}
}

func makeScanCommand() *cli.Command {

	/*
		db : "root:1111@tcp(127.0.0.1:3306)/ether_tx",
		web3provider : "https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161",
		toblock : 0,
		fromBlock :1000000,
	*/

	return &cli.Command{
		Name:    "blockscan",
		Aliases: []string{"bs"},
		Usage:   "specific net block scan and record",
		Flags: []cli.Flag{
			&cli.Uint64Flag{
				Name:     "fromblock",
				Usage:    "scan fromBlock",
				Aliases:  []string{"f"},
				Required: true,
			},
			&cli.Uint64Flag{
				Name:     "toblock",
				Usage:    "scan toBlock",
				Aliases:  []string{"t"},
				Required: true,
			},
			&cli.StringSliceFlag{
				Name:     "web3",
				Usage:    "web3 provider http/https url",
				Aliases:  []string{"w"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "db",
				Usage:    "record target database connection string",
				Aliases:  []string{"d"},
				Required: true,
			},
			&cli.IntFlag{
				Name:     "scan-worker-count",
				Usage:    "block scan worker count",
				Aliases:  []string{"sw"},
				Required: false,
			},
			&cli.IntFlag{
				Name:     "write-worker-count",
				Usage:    "block write worker count",
				Aliases:  []string{"ww"},
				Required: false,
			},
			&cli.IntFlag{
				Name:     "confirm-worker-count",
				Usage:    "block confirm worker count",
				Aliases:  []string{"cw"},
				Required: false,
			},
		},
		Action: func(c *cli.Context) error {

			fromBlock := c.Uint64("fromblock")
			toBlock := c.Uint64("toblock")
			web3Providers := c.StringSlice("web3")
			dbConnectString := c.String("db")

			blockScanWorker := c.Int("scan-worker-count")
			dbWriteWorker := c.Int("write-worker-count")
			dbConfirmWorker := c.Int("confirm-worker-count")

			configure := scan.BlockScanConfigure{
				BlockScanWorker: blockScanWorker,
				DBWriteWorker:   dbWriteWorker,
				DBConfirmWorker: dbConfirmWorker,
				Web3Providers:   web3Providers,
				DbConnString:    dbConnectString,
			}

			return scanFunc(fromBlock, toBlock, configure)
		},
	}
}

func checkLocalGethNode() {

	client, err := jsonrpc.NewClient("http://172.30.1.202:7550")
	if err != nil {
		fmt.Println(err)
		return
	}

	pearCount, _ := client.Net().PeerCount()
	fmt.Println("pear Count = ", pearCount)

	block, err := client.Eth().GetBlockByNumber(5000000, true)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(block)
}

func check_provider() {

	//0xe3bd135eae0a9b94067214a120ea4e9fc11a7fd1ab2e04ff06d3c15f75b336ac

	//https://api.mycryptoapi.com/eth
	//https://nodes.mewapi.io/rpc/eth

	client, _ := jsonrpc.NewClient("http://172.30.1.202:7547")
	pearCount, _ := client.Net().PeerCount()
	fmt.Printf("listening %v\n", pearCount)

	lastBlock, _ := client.Eth().BlockNumber()
	fmt.Printf("block num : %v\n", lastBlock)

	//1270000
	block, err := client.Eth().GetBlockByNumber(0, true)
	if err != nil {
		fmt.Println(err)
		return
	}

	if block == nil {
		fmt.Println("block is nil")
		return
	}

	fmt.Printf("block hash = %v, tx count = %v \n", block.Hash, len(block.Transactions))

	for _, tx := range block.Transactions {
		receipt, err := client.Eth().GetTransactionReceipt(tx.Hash)
		if err != nil {
			fmt.Println(err)
			return
		}

		if receipt == nil {
			fmt.Println("failed receipt is null")
			continue
		}

		fmt.Println("success = ", receipt.TransactionHash)
	}

	receipt, err := client.Eth().GetTransactionReceipt(web3.HexToHash("0xe65bad7fe7fe83da6a12cf0d72564f7188d8c1ab866b8802a3b40afcb8344792"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("receipt")
	fmt.Println(receipt)

}

func test_scanFunc() {

	//4001000
	//4501000

	configure := scan.BlockScanConfigure{
		BlockScanWorker: 1024,
		DBWriteWorker:   256,
		DBConfirmWorker: 64,
		DbConnString:    "root:1111@tcp(127.0.0.1:3306)/ether_tx",
		Web3Providers: []string{
			"http://172.30.1.202:7541",
			"http://172.30.1.202:7542",
			"http://172.30.1.202:7543",
			"http://172.30.1.202:7544",
			"http://172.30.1.202:7545",
			"http://172.30.1.202:7546",
			//"http://172.30.1.202:7547",
			//"http://172.30.1.202:7548",
			"http://172.30.1.202:7549",
			"http://172.30.1.202:7550",

			"https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161",
			//"https://api.mycryptoapi.com/eth",
			//"https://nodes.mewapi.io/rpc/eth",
		},
	}

	err := scanFunc(4501000, 4501010, configure)
	if err != nil {
		fmt.Println(err)
	}
}

func main() {

	cores := runtime.NumCPU()
	runtime.GOMAXPROCS(cores)

	//test_scanFunc()

	//check_provider()
	//return

	////////
	/*
		r1, err := regexp.Compile("dial tcp ([0-9.:]+): socket: too many open files")
		if err != nil {
			fmt.Println(err)
			return
		}

		m1 := r1.MatchString("dial tcp 127.0.0.1:3306: socket: too many open files")
		fmt.Printf("macthed = %v\n", m1)

		r2, err := regexp.Compile("")
	*/

	// check
	// 0xbd7c924ab0b345466222dd6858907b94cb6b1250249454efc80425f66a2edd58
	// 0xb3a2389a5c60a830ab54117891014828d2e79a4a2b6592f8b56c9182a03825a7

	app := &cli.App{
		Name:  "ethereum scan and query",
		Usage: "ethereum scan and query",
		Flags: []cli.Flag{},
		Commands: []*cli.Command{
			makeLatestBlockCommand(),
			makeScanCommand(),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}
