
# Basic Wrapper for the Cassandra 0.8 Thrift Bindings.

I wanted to play with Cassandra from erlang and the default Thrift Bindings are horrible, so I made this. Might be just as horrible but works for me.


## EXAMPLE

    C = cassandra_client:connect([{host, "localhost"}, {port, 9160}, {keyspace, "test"}]),

    {ok, Version} = C:describe_version(),

    ConsistencyLevel = one,

    ok = C:mutate([
        {"row", "cf", {insert, [{"col", "value"}]}
    ], ConsistencyLevel),

    {ok, Columns} = C:get_column_slice("row", "cf", [{count, 10}], ConsistencyLevel),

    % return values from cassandra are binaries

    Columns = [{<<"col">>, <<"value">>}].

## TODO

* SuperColumns
* Error Handling (pure let-it-crash for now)
* Ring Detection, Client Pooling, Reconnects, etc.

