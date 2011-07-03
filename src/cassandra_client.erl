-module(cassandra_client).
-behaviour(gen_server).

-export([
    connect/0,
    connect/1,
    start_link/1,
    set_keyspace/2,
    describe_version/1,
    insert/5,
    incr/6,
    get_column/5,
    get_column_slice/5,
    get_counter/5,
    get_counter_slice/5,
    mutate/3
  ]).

-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include("utils.hrl").
-include("cassandra_types.hrl").

-record(state, {options, client}).

-type client() :: pid().
-type row() :: iolist().
-type column_family() :: iolist() | atom().

-type column_name() :: iolist().
-type column_value() :: iolist().
-type ttl() :: non_neg_integer().

-type column() :: {column_name(), column_value(), ttl()} | {column_name(), column_value()}.

-type mutation() :: {row(), column_family(), [column_mutation()]}.
-type column_mutation() :: {insert, [column()]} | {incr, [column()]}.

-type slice_option() :: {start, column_name()} | {finish, column_name()} | {count, non_neg_integer()} | reversed.
-type connect_option() :: {host, iolist()} | {port, non_neg_integer()} | {keyspace, iolist()}.

-type clevel() :: one.

-spec set_keyspace(client(), iolist()) -> ok.

set_keyspace(Client, Name) ->
  {ok, ok} = call(Client, set_keyspace, [Name]),
  ok.

-spec describe_version(client()) -> {ok, binary()}.

describe_version(Client) ->
  call(Client, describe_version, []).

-spec insert(client(), row(), column_family(), column(), clevel()) -> ok.

insert(Client, Row, CF, Column, CL) ->
  {ok, ok} = call(Client, insert, [
      Row,
      #columnParent{column_family = translate_column_family(CF)},
      Column,
      translate_consistency_level(CL)
    ]),
  ok.

-spec incr(client(), row(), column_family(), column_name(), integer(), clevel()) -> ok.

incr(Client, Row, CF, Name, Value, CL) ->
  {ok, ok} = call(Client, add, [
      Row,
      #columnParent{column_family = translate_column_family(CF)},
      #counterColumn{name = Name, value = Value},
      translate_consistency_level(CL)
    ]),
  ok.

-spec get_column(client(), row(), column_family(), column_name(), clevel()) -> not_found | {ok, column()}.

get_column(Client, Row, CF, Name, CL) ->
  CallResult = call(Client, get, [
      Row,
      #columnPath{column_family = translate_column_family(CF), column = Name},
      translate_consistency_level(CL)
    ]),

  case CallResult of
    {ok, #columnOrSuperColumn{column = Column}} ->
      {ok, {Column#column.name, Column#column.value, Column#column.timestamp}};

    {exception, {notFoundException}} ->
      not_found
  end.

-spec get_counter(client(), row(), column_family(), column_name(), clevel()) -> not_found | {ok, column()}.

get_counter(Client, Row, CF, Name, CL) ->
  CallResult  = call(Client, get, [
      Row,
      #columnPath{column_family = translate_column_family(CF), column = Name},
      translate_consistency_level(CL)
    ]),

  case CallResult of
    {ok, #columnOrSuperColumn{counter_column = Column}} ->
      {ok, {Column#counterColumn.name, Column#counterColumn.value}};

    {exception, {notFoundException}} ->
      not_found
  end.

-spec get_column_slice(client(), row(), column_family(), [slice_option()], clevel()) -> {ok, [column()]}.

get_column_slice(Client, Row, CF, Options, CL) ->
  {ok, Result} = get_slice(Client, Row, CF, Options, CL),

  Columns = [ {Name, Value, TS} || #columnOrSuperColumn{column = #column{name = Name, value = Value, timestamp = TS}} <- Result ],
  {ok, Columns}.

-spec get_counter_slice(client(), row(), column_family(), [slice_option()], clevel()) -> {ok, [column()]}.

get_counter_slice(Client, Row, CF, Options, CL) ->
  {ok, Result} = get_slice(Client, Row, CF, Options, CL),

  Columns = [ {Name, Value} || #columnOrSuperColumn{counter_column = #counterColumn{name = Name, value = Value}} <- Result ],
  {ok, Columns}.

-spec mutate(client(), [mutation()], clevel()) -> ok.

mutate(Client, Mutations, CL) when is_list(Mutations) ->
  {ok, ok} = call(Client, batch_mutate, [
      make_mutation_map(Mutations),
      translate_consistency_level(CL)
    ]),
  ok.

call(Client, Method, Args) when is_atom(Method) andalso is_list(Args)->
  gen_server:call(Client, {call, Method, Args}).

-spec connect() -> client().

connect() ->
  connect([ {keyspace, "metrics"} ]).

-spec connect([cassandra_client:connect_option()]) -> client().

connect(Options) ->
  {ok, Pid} = cassandra_client:start_link(Options),
  cassandra_client_util:new(Pid).

-spec start_link([connect_option()]) -> {ok, pid()}.

start_link(Options) when is_list(Options) ->
  {ok, Pid} = gen_server:start_link(?MODULE, Options, []),
  {ok, Pid}.

init(Options) ->
  {ok, Client} = connect_client(Options),
  {ok, #state{options = Options, client = Client}}.

handle_call({call, Method, Args}, _From, #state{client = C0} = State) ->
  {C1, Result} = (catch thrift_client:call(C0, Method, Args)),
  {reply, Result, State#state{client = C1}};

handle_call(Msg, From, State) ->
  ?LOG({call, Msg, From}),
  {reply, ignored, State}.

handle_cast(Msg, State) ->
  ?LOG({cast, Msg}),
  {noreply, State}.

handle_info(Msg, State) ->
  ?LOG({info, Msg}),
  {noreply, State}.

terminate(Reason, State) ->
  ?LOG({terminate, Reason, State}),
  ok.

code_change(_, _, State) ->
  State.

% connects the client and selects a keyspace (when given)
connect_client(Options) ->
  Host = proplists:get_value(host, Options, "localhost"),
  Port = proplists:get_value(port, Options, 9160),

  {ok, C0} = thrift_client_util:new(Host, Port, cassandra_thrift, [{framed, true}]),

  case proplists:lookup(keyspace, Options) of
    none ->
      {ok, C0};

    {keyspace, Keyspace} ->
      {C1, {ok, ok}} = thrift_client:call(C0, set_keyspace, [Keyspace]),
      {ok, C1}
  end.

% not exposed directly, use get_counter_slice, get_column_slice
get_slice(Client, Row, CF, Options, CL) ->
  call(Client, get_slice, [
      Row,
      #columnParent{column_family = translate_column_family(CF)},
      make_slice_range(Options),
      translate_consistency_level(CL)
    ]
  ).

make_mutations({insert, Columns}) ->
  [ #mutation{column_or_supercolumn = make_column(Column)} || Column <- Columns ];

make_mutations({incr, Columns}) ->
  [ #mutation{column_or_supercolumn = make_counter_column(Column)} || Column <- Columns ].

make_mutation_map(Input) when is_list(Input) ->
  Mutations = [ {iolist_to_binary(Row), translate_column_family(CF), make_mutations(Mutation)} || {Row, CF, Mutation} <- Input ],

  RowDict = lists:foldl(fun({Row, CF, M}, Dict) -> dict:append(Row, {CF, M}, Dict) end, dict:new(), Mutations),
  
  Result = dict:map(fun(_, Value) ->
    lists:foldl(fun({CF, M}, Dict) ->
      dict:append_list(CF, M, Dict)
    end, dict:new(), Value)
  end, RowDict),

  Result.


time_ms() ->
  time_ms(now()).

time_ms({MegaSec, Ts, Ms}) ->
  (((MegaSec * 1000000) + Ts) * 1000) + round(Ms / 1000).

make_column({Name, Value}) ->
  make_column({Name, Value, undefined});

make_column({Name, Value, TTL}) ->
  #columnOrSuperColumn{column = #column{name = Name, value = Value, timestamp = time_ms(), ttl = TTL}}.

make_counter_column({Name, Value}) ->
  #columnOrSuperColumn{counter_column = #counterColumn{name = Name, value = Value}}.

make_slice_range(Options) ->
  make_slice_range(Options, #sliceRange{start = <<"">>, finish = <<"">>, reversed = false, count = 100}).

make_slice_range([], SR) ->
  #slicePredicate{slice_range = SR};

make_slice_range([{count, Num} | Options], SR) when is_integer(Num) ->
  make_slice_range(Options, SR#sliceRange{count = Num});

make_slice_range([reversed | Options], SR) ->
  make_slice_range(Options, SR#sliceRange{reversed = true});

make_slice_range([{start, Bin} | Options], SR) ->
  make_slice_range(Options, SR#sliceRange{start = Bin});

make_slice_range([{finish, Bin} | Options], SR) ->
  make_slice_range(Options, SR#sliceRange{finish = Bin});

make_slice_range([Unknown | _Options], _SR) ->
  throw({invalid_slice_range_option, Unknown}).

translate_column_family(CF) when is_atom(CF) ->
  translate_column_family(atom_to_list(CF));

translate_column_family(CF) ->
  iolist_to_binary(CF).

translate_consistency_level(one) ->
  ?cassandra_ConsistencyLevel_ONE.

%%
%% Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_slice_range_test() ->
  Options = [ {start, "a"}, {finish, "b"}, {count, 10}, reversed ],

  SP = make_slice_range(Options),
  SR = SP#slicePredicate.slice_range,

  ?assertEqual(SR#sliceRange.start, "a"),
  ?assertEqual(SR#sliceRange.finish, "b"),
  ?assertEqual(SR#sliceRange.reversed, true),
  ?assertEqual(SR#sliceRange.count, 10),

  ok.

mutation_map_test() ->
  Result = make_mutation_map([
      {"test", data, {insert, [{test1, value}, {test2, value}, {test3, value}]}},
      {"test2", data, {insert, [{test1, value}, {test2, value}]}}
    ]),

  % map transforms keys to binaries
  % there should be one entry for test and test2
  {ok, Test} = dict:find(<<"test">>, Result),
  ?assertEqual(1, dict:size(Test)),

  % test should have a list values with 2 entries
  {ok, List1} = dict:find(<<"data">>, Test),
  ?assertEqual(3, length(List1)),

  % deconstructing the actual cassandra mutation would drive me insane,
  % someone more patient should actually test if the mutation is correct
  % ?debugVal(List1),

  {ok, Test2} = dict:find(<<"test2">>, Result),
  ?assertEqual(1, dict:size(Test2)),

  {ok, List2} = dict:find(<<"data">>, Test2),
  ?assertEqual(2, length(List2)),

  ok.

slice_test() ->
  C = connect(),

  ok = C:mutate([
      {"slice_test", data, {insert, [ {"test1", <<1:64/float>>}, {"test2", <<2:64/float>>}, {"test3", <<3:64/float>>} ]}}
    ], one),

  {ok, Columns} = C:get_column_slice("slice_test", data, [{count, 3}], one),

  ?assertEqual(3, length(Columns)),

  Names = [ Name || {Name, _, _} <- Columns ],

  ?assertEqual(Names, [<<"test1">>, <<"test2">>, <<"test3">>]),

  Values = [ Value || {_, <<Value:64/float>>, _} <- Columns ],

  ?assertEqual(Values, [ 1.0, 2.0, 3.0 ]),
  ok.

simple_client_test() ->
  C = connect(),

  {ok, Version} = C:describe_version(),
  ?assertEqual(<<"19.10.0">>, Version),

  ok = C:mutate([
      {"test", data, {insert, [{"test", "world"}]}}
    ], one),

  {ok, {Name, Value, _Timestamp}} = C:get_column("test", data, "test", one), 

  % cassandra returns actual binaries
  ?assertEqual({<<"test">>, <<"world">>}, {Name, Value}),
  ok.

counter_test() ->
  C = connect(),

  Before = case C:get_counter("counter_test", counters, "count", one) of
    {ok, {_, Current}} ->
      Current;

    not_found ->
      0
  end,

  ok = C:incr("counter_test", counters, "count", 1, one),

  {ok, Column} = C:get_counter("counter_test", counters, "count", one),
  ?assertEqual({<<"count">>, Before + 1}, Column),

  ok = C:mutate([
      {"counter_test", counters, {incr, [{"count", -1}]}}
    ], one),

  {ok, Column2} = C:get_counter("counter_test", counters, "count", one),
  ?assertEqual({<<"count">>, Before}, Column2),

  ok.

-endif.
