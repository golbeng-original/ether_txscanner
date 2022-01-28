#!/bin/bash

OS=$(uname)
SCAN_PATH="./bin/linux/ether_txscanner"

if [ "${OS}" = "Darwin" ]; then
    SCAN_PATH="./bin/darwin/ether_txscanner"
elif [ "${OS}" = "Linux" ]; then
    SCAN_PATH="./bin/linux/ether_txscanner"
fi

echo "from block : "
read FROMBLOCK

echo "to block : "
read TOBLOCK

#--web3 "http://172.30.1.202:7547" \

${SCAN_PATH} blockscan \
--fromblock ${FROMBLOCK} \
--toblock ${TOBLOCK} \
--web3 "http://172.30.1.202:7541" \
--web3 "http://172.30.1.202:7542" \
--web3 "http://172.30.1.202:7543" \
--web3 "http://172.30.1.202:7544" \
--web3 "http://172.30.1.202:7545" \
--web3 "http://172.30.1.202:7546" \
--web3 "http://172.30.1.202:7548" \
--web3 "http://172.30.1.202:7549" \
--web3 "http://172.30.1.202:7550" \
--web3 "https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161" \
--db "root:1111@tcp(127.0.0.1:3306)/ether_tx" \
--scan-worker-count 1024 \
--write-worker-count 256 \
--confirm-worker-count 64