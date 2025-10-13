The CDC future (PHOENX-7001) uses a global uncovered time (PHOENIX_ROW_TIMESTAM()) based index. Such an index will likely create hot spotting during writes. This is because the same index region will keep getting updated as the row key of the index table is PHOENIX_ROW_TIMESTAMP() + data table row key.

The same hot spotting can happen during reads as a small subset of index regions can be used for a given time range. For example, the most recent changes will be retrieved through one or two index regions.

To address these hot spotting issues, PHOENX-7001 suggests salting the index. There are three main issues with salting.

The first one is that the number of salt buckets is static and needs to be determined when the index is created.

The second is that salting does not work well with batch writes as it results in breaking a batch of writes into separate mini batches, one for each salt bucket. This leads to using more client threads and server RPC handlers, one for each salt bucket.

The last issue is that the salt buckets are not visible to applications and thus they cannot take advantage of the parallelism that comes with salting during reads. For example, there is no way for applications to use multiple threads, one thread for each salt bucket, for their queries.

To address all these issues that come with salting, this PR introduces a built-in function for CDC indexes called PARTITION_ID(). PARTITION_ID() will be the prefix of an index row key (= PARTITION_ID() + PHOENIX_ROW_TIMESTAMP() + data table row key). PARTITION_ID() will identify the data table region of the data table row key. PARTITION_ID() can be the encoded name of the data table region.

Like PHOENIX_ROW_TIMESTAMP(), PARTITION_ID() can be used in CDC index queries.

By including PARTITION_ID() in the row key of an index table, we essentially create the effect of local index such that all index mutations for a given data table region are written to one index region determined by the PARITION_ID(). However, here we will not have the local index problem with region splits where copying index rows during data table region splits is required.

@kadirozde
