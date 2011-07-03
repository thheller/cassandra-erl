-module(cassandra_client_util, [Client]).

-export([
    describe_version/0,
    mutate/2,
    get_column/4,
    get_counter/4,
    get_column_slice/4,
    get_counter_slice/4,
    incr/5
  ]).

describe_version() ->
  cassandra_client:describe_version(Client).

get_column(Row, CF, Name, CL) ->
  cassandra_client:get_column(Client, Row, CF, Name, CL).

get_counter(Row, CF, Name, CL) ->
  cassandra_client:get_counter(Client, Row, CF, Name, CL).

mutate(Mutations, CL) ->
  cassandra_client:mutate(Client, Mutations, CL).

get_counter_slice(Row, CF, Options, CL) ->
  cassandra_client:get_counter_slice(Client, Row, CF, Options, CL).

get_column_slice(Row, CF, Options, CL) ->
  cassandra_client:get_column_slice(Client, Row, CF, Options, CL).

incr(Row, CF, Name, Value, CL) ->
  cassandra_client:incr(Client, Row, CF, Name, Value, CL).


