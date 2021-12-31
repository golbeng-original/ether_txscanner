use ether_tx;

SET foreign_key_checks = 0;

DROP TABLE block;
DROP TABLE tx;
DROP TABLE tx_log;
DROP TABLE topic;
DROP TABLE erc721_transfer_topic;

SET foreign_key_checks = 1;