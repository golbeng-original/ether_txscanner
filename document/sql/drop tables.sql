use ether_tx;

SET foreign_key_checks = 0;

SET SQL_SAFE_UPDATES = 0;
TRUNCATE ether_tx.block;
TRUNCATE ether_tx.tx;
TRUNCATE ether_tx.tx_log;
TRUNCATE ether_tx.tx_log_topic;
TRUNCATE ether_tx.erc721_transfer_topic;
SET SQL_SAFE_UPDATES = 1;

DROP TABLE ether_tx.block;
DROP TABLE ether_tx.tx;
DROP TABLE ether_tx.tx_log;
DROP TABLE ether_tx.tx_log_topic;
DROP TABLE ether_tx.erc721_transfer_topic;

SET foreign_key_checks = 1;