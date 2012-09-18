
-module(riak_kv_lowkeydb_backend).
%%-behavior(riak_kv_backend).

%% Riak Storage Backend API
-export([api_version/0,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-record(state, {folder_ref :: string()}).

-type state() :: #state{}.
-type config() :: [].

-define(API_VERSION, 1).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API and a capabilities list.
%% The current valid capabilities are async_fold
%% and indexes.
-spec api_version() -> {integer(), [atom()]}.
api_version() -> 
	{?API_VERSION, [alpha]}.

%% @doc Start the backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, _Config) -> 
  Folder = "~/riak/dev/dev1/data/lowkey",%app_helper:get_prop_or_env(data_root, Config, lowkey),
  NodeFolder = filename:join(Folder, integer_to_list(Partition)),
  lager:debug("Folder: ~p", [NodeFolder]),
  filelib:ensure_dir(filename:join(NodeFolder, "dummy")),
  {ok, #state{folder_ref=NodeFolder}}.

%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(_State) ->
    ok.

%% @doc Retrieve an object from the backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(_Bucket, _Key, State) ->
	{error, not_found, State}.


%% @doc Insert an object into the backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.

put(_Bucket, _Key, _IndexSpec, _Value, State) ->
	{ok, State}.

%% @doc Delete an object from the backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(_Bucket, _Object, _IndexSpecs, State) ->
	{ok, State}.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(_Fun, _Acc, _Opts, _State) ->
	{ok, doki}.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(_FoldKeyFun, _Acc, _Opts, _State) ->
	{ok, doki}.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(_FoldObjectsFun, _Acc, _Opts, _State) ->
	{ok, doki}.

%% @doc Delete all objects from this backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(State) ->
	{ok, State}.

%% @doc Returns true if this backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(_State) ->
	false.

%% @doc Get the status information for this backend
-spec status(state()) -> [{atom(), term()}].
status(_Status) ->
	[{stats, "Ohh yea baby"}].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.