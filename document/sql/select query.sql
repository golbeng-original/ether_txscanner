SELECT count(*) FROM ether_tx.block;

SELECT * FROM ether_tx.block WHERE scaned_transaction = false LIMIT 1000;

SELECT COUNT(*) FROM ether_tx.block WHERE scaned_transaction = false
AND block_index >= 4501000
AND block_index <= 4501010;

#block
SELECT * FROM ether_tx.block WHERE block_index >= 2001395 AND block_index <= 2001400;

#tx
SELECT * FROM ether_tx.tx WHERE scaned_receipt = true AND block_index >= 2001300 and block_index <= 2001320;

SELECT * FROM ether_tx.tx WHERE block_index = 2001011;

# txLog
SELECT tx.tx_hash, tx.log_count, log.log_index, log.scaned_log FROM ether_tx.tx tx
JOIN ether_tx.tx_log log on tx.tx_hash = log.tx_hash
WHERE tx.block_index = 2001123;

SELECT tx.tx_hash, tx.log_count, log.log_index FROM ether_tx.tx tx 
JOIN ether_tx.tx_log log ON tx.tx_hash = log.tx_hash 
WHERE tx.scaned_receipt = false 
AND log.scaned_log = true 
AND tx.block_index >= 0 AND tx.block_index <= 15000000;

#SELECT * FROM ether_tx.block WHERE scaned_transaction = false ORDER BY block_index limit 10;

#SELECT * FROM ether_tx.block WHERE block_index >= 517012 and block_index <= 517012;

#SELECT hex(tx_hash), log_index,scaned_log FROM ether_tx.tx_log WHERE tx_hash = unhex('4dfd722e46aaef2eea6b042afd862e77d7cbcc5c2df3b9a237ac09e7d33282f5');

#SELECT * FROM ether_tx.tx WHERE tx_hash = unhex('4DFD722E46AAEF2EEA6B042AFD862E77D7CBCC5C2DF3B9A237AC09E7D33282F5')

SELECT block_index, hex(tx_hash), scaned_receipt FROM ether_tx.tx WHERE block_index = 963070;

SELECT hex(tx.tx_hash), tx.log_count, log.log_index FROM ether_tx.tx tx 
JOIN ether_tx.tx_log log ON tx.tx_hash = log.tx_hash
WHERE
tx.scaned_receipt = false
AND
log.scaned_log = true;

#where tx_hash = unhex('16BAE3B54E7FB2EB19AD1FD5B01C0A694E017D8A5FEB1F2D0566EC5798AD253C')

SELECT * FROM ether_tx.tx_log where tx_hash = unhex('16BAE3B54E7FB2EB19AD1FD5B01C0A694E017D8A5FEB1F2D0566EC5798AD253C')

#511106

# 511674
# 523618