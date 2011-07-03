-ifndef(_cassandra_types_included).
-define(_cassandra_types_included, yeah).

-define(cassandra_ConsistencyLevel_ONE, 1).
-define(cassandra_ConsistencyLevel_QUORUM, 2).
-define(cassandra_ConsistencyLevel_LOCAL_QUORUM, 3).
-define(cassandra_ConsistencyLevel_EACH_QUORUM, 4).
-define(cassandra_ConsistencyLevel_ALL, 5).
-define(cassandra_ConsistencyLevel_ANY, 6).
-define(cassandra_ConsistencyLevel_TWO, 7).
-define(cassandra_ConsistencyLevel_THREE, 8).

-define(cassandra_IndexOperator_EQ, 0).
-define(cassandra_IndexOperator_GTE, 1).
-define(cassandra_IndexOperator_GT, 2).
-define(cassandra_IndexOperator_LTE, 3).
-define(cassandra_IndexOperator_LT, 4).

-define(cassandra_IndexType_KEYS, 0).

-define(cassandra_Compression_GZIP, 1).
-define(cassandra_Compression_NONE, 2).

-define(cassandra_CqlResultType_ROWS, 1).
-define(cassandra_CqlResultType_VOID, 2).
-define(cassandra_CqlResultType_INT, 3).

-record(column, {name, value, timestamp, ttl}).

-record(superColumn, {name, columns}).

-record(counterColumn, {name, value}).

-record(counterSuperColumn, {name, columns}).

-record(columnOrSuperColumn, {column, super_column, counter_column, counter_super_column}).

-record(notFoundException, {}).

-record(invalidRequestException, {why}).

-record(unavailableException, {}).

-record(timedOutException, {}).

-record(authenticationException, {why}).

-record(authorizationException, {why}).

-record(schemaDisagreementException, {}).

-record(columnParent, {column_family, super_column}).

-record(columnPath, {column_family, super_column, column}).

-record(sliceRange, {start, finish, reversed, count}).

-record(slicePredicate, {column_names, slice_range}).

-record(indexExpression, {column_name, op, value}).

-record(indexClause, {expressions, start_key, count}).

-record(keyRange, {start_key, end_key, start_token, end_token, count}).

-record(keySlice, {key, columns}).

-record(keyCount, {key, count}).

-record(deletion, {timestamp, super_column, predicate}).

-record(mutation, {column_or_supercolumn, deletion}).

-record(tokenRange, {start_token, end_token, endpoints}).

-record(authenticationRequest, {credentials}).

-record(columnDef, {name, validation_class, index_type, index_name}).

-record(cfDef, {keyspace, name, column_type, comparator_type, subcomparator_type, comment, row_cache_size, key_cache_size, read_repair_chance, column_metadata, gc_grace_seconds, default_validation_class, id, min_compaction_threshold, max_compaction_threshold, row_cache_save_period_in_seconds, key_cache_save_period_in_seconds, memtable_flush_after_mins, memtable_throughput_in_mb, memtable_operations_in_millions, replicate_on_write, merge_shards_chance, key_validation_class, row_cache_provider, key_alias}).

-record(ksDef, {name, strategy_class, strategy_options, replication_factor, cf_defs}).

-record(cqlRow, {key, columns}).

-record(cqlResult, {type, rows, num}).

-endif.
