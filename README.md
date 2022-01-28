ether_txscanner
===============

ether-txscanner DB SCHME
------------------------
- document/sql/create table.sql 참고 및 생성

<br>

ether-txscanner CLI
-------------------
- blockscan
    - sh/blockscan.sh 참고
- get-latestblock
    - get-latestblock --web3 "web3 provider"

ether-txscanner BUILD
---------------------
- macos build
    - go build -o ./bin/darwin/ether-txscanner
- linux build
    - GOOS=linux GOARCH=amd64 go build -o ./bin/linux/ether-txscanner