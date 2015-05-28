CREATE TABLE t_transaction(
	offset				NUMERIC(20) PRIMARY KEY,
	tranStateOffset  NUMERIC(20),
	pgroupHashcode		NUMERIC(10),
	msgSize NUMERIC(10),
	timestamp NUMERIC(14)
)
