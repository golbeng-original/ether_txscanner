use ether_tx;

CREATE TABLE IF NOT EXISTS `block`(
	`block_index` int unsigned not null,
	`block_hash` varbinary(256) not null,
    `block_timestamp` timestamp not null,
    
    `transction_count` int unsigned default 0,
    `scaned_transaction` bool default false,
    
    PRIMARY KEY(`block_index`, `block_hash`)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `block` ADD INDEX `tx_log_block_index` (`block_index`);

/*
CREATE TABLE IF NOT EXISTS `error_block` (
	`block_hash` varbinary(256) not null,
    PRIMARY KEY(`block_hash`)
);

CREATE TABLE IF NOT EXISTS `error_block_index` (
	`block_index` bigint unsigned not null,
    PRIMARY KEY(`error_block_index`)
);
*/

CREATE TABLE IF NOT EXISTS `tx`(
	`block_index` int unsigned not null,
	`tx_hash` varbinary(256) not null,
    
    `from_address` varbinary(256) not null,
    `to_address` varbinary(256) not null,
    `eth_value` bigint unsigned not null,
    
	`log_count` int unsigned default 0,
    `scaned_receipt` bool default(false),
    
    PRIMARY KEY(`tx_hash`)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `tx` ADD INDEX `tx_block_index` (`block_index`);

/*
CREATE TABLE IF NOT EXISTS `error_tx` (
	`tx_hash` varbinary(256) not null,
    PRIMARY KEY(`tx_hash`)
);
*/

CREATE TABLE IF NOT EXISTS `tx_log` (
	`tx_hash` varbinary(256) not null,
    `log_index` bigint unsigned not null,
    `log_address` varbinary(256) not null,
    `topic_count` int unsigned default 0,
	`scaned_log` bool default(false),
    
    PRIMARY KEY(`tx_hash`, `log_index`)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
ALTER TABLE `tx_log` ADD INDEX `tx_log_index` (`tx_hash`, `log_index`);


CREATE TABLE IF NOT EXISTS `tx_log_topic`(
	`tx_hash` varbinary(256) not null,
    `log_index` bigint unsigned not null,
    `topic_index` int unsigned not null,
    `topic` varbinary(256) not null,
    
    PRIMARY KEY(`tx_hash`, `log_index`, `topic_index`)

) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `tx_log` ADD INDEX `tx_log_topic_index` (`tx_hash`, `log_index`);

CREATE TABLE IF NOT EXISTS `erc721_transfer_topic`(
    `tx_hash` varbinary(256) not null,
    `log_index` int not null,
    `topic_index` int not null,
	
    `from` varbinary(256) not null,
    `to` varbinary(256) not null,
    `tokenId` int not null,
    
    PRIMARY KEY(`tx_hash`, `log_index`, `topic_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
