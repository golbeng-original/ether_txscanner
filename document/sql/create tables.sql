use ether_tx;

CREATE TABLE IF NOT EXISTS `block`(
	`block_hash` varbinary(256) not null,
	`block_num` int not null,
   
    `block_timestamp` timestamp not null,
    
    PRIMARY KEY(`block_hash`)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `scaned_block` (
	`block_hash` varbinary(256) not null,
    PRIMARY KEY(`block_hash`)
);

CREATE TABLE IF NOT EXISTS `tx`(
	`tx_hash` varbinary(256) not null,
	`block_hash` varbinary(256) not null,
    
    `from_address` varbinary(256) not null,
    `to_address` varbinary(256) not null,
    `eth_value` int not null,
    
    PRIMARY KEY(`tx_hash`, `block_hash`),
    
	FOREIGN KEY (`block_hash`) REFERENCES block (`block_hash`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `tx_log`(
	`tx_hash` varbinary(256) not null,
    `log_index` int not null,
	`address` varbinary(256) not null,

	PRIMARY KEY(`tx_hash`, `log_index`),

	FOREIGN KEY (`tx_hash`) REFERENCES `tx` (`tx_hash`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `erc721_transfer_topic`(
    `tx_hash` varbinary(256) not null,
    `log_index` int not null,
    `topic_index` int not null,
	
    `from` varbinary(256) not null,
    `to` varbinary(256) not null,
    `tokenId` int not null,
    
    PRIMARY KEY(`tx_hash`, `log_index`, `topic_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;