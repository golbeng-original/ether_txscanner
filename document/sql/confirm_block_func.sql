
CREATE FUNCTION `confirm_block` (
	check_block_index INTEGER
)
RETURNS INTEGER
BEGIN

	DECLARE init_tx_count INTEGER;
	DECLARE confirm_tx_count INTEGER;
    DECLARE ret INTEGER DEFAULT 0;

	SELECT transction_count FROM block WHERE block_index = check_block_index
    INTO init_tx_count;
    
    SELECT count(*) FROM tx WHERE block_index = check_block_index and scaned_receipt = true 
	INTO confirm_tx_count;

	IF init_tx_count = confirm_tx_count THEN
		SET ret = 1;
    END IF;

RETURN ret;
END
