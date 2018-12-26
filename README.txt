Step 1: In your cassandra.yaml file,set incremental_backup to true.

Step 2: Start your cassandra and elasticsearch instances(If not already running).

Step 3: The configurations.yaml file is important.Make the appropriate settings in that file.

Step 4: Run cassandraToES.sh .(This indexes all data already present in the database.)
If you have placed the configurations.yaml file at a different place,pass that as the only argument.

Note:The cassandra data is modelled in ElasticSearch as follows-
	1.The keyspace name becomes the index name.
	2.The columnFamily name becomes the type name.
	3. The row_key becomes the id of the document.
