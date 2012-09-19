
-module(riak_kv_lowkeydb_backend).
%-behavior(riak_kv_backend).

-import(file_utils).

%% Riak Storage Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
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
-type config() :: [any()].

-define(API_VERSION, 1).
-define(CAPABILITIES, [indexes]).

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

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
  {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
  {ok, ?CAPABILITIES}.

%% @doc Start the backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
  Folder = app_helper:get_prop_or_env(data_root, Config, lowkeydb),
  PartitionRootFolder = filename:join([Folder, integer_to_list(Partition)]),
  PartitionDataFolder = filename:join([Folder, integer_to_list(Partition), "buckets"]),
  PartitionIndexFolder = filename:join([Folder, integer_to_list(Partition), "indeces"]),
  filelib:ensure_dir(filename:join(PartitionDataFolder, "dummy")),
  filelib:ensure_dir(filename:join(PartitionIndexFolder, "dummy")),
  {ok, #state{folder_ref=PartitionRootFolder}}.

%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(_State) ->
    ok.

%% @doc Retrieve an object from the backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{folder_ref=Folder}=State) ->
  KeyFile = filename:join([Folder, "buckets", Bucket, Key]),
  case file:read_file(KeyFile) of
  {ok, Value} -> {ok, Value, State};
  {error, enoent} -> {error, not_found, State};
  {error, Reason} -> {error, Reason, State}
  end.


%% @doc Insert an object into the backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.

put(Bucket,  Key, IndexSpec, Value, #state{folder_ref=Folder}=State) ->
  RawFile = filename:join([Folder, "buckets", Bucket, Key]),
  filelib:ensure_dir(RawFile),
  ContentFile = filename:join([Folder, Bucket, string:concat(binary_to_list(Key), "_content")]),
  [ContentTerm] = element(4, binary_to_term(Value)), %% unpack list by using []
  ContentAsIntArray = binary_to_list(element(3, ContentTerm)),
  file:write_file(RawFile, Value),
  file:write_file(ContentFile, ContentAsIntArray),

  %% update indeces
  apply_index_spec(IndexSpec, Folder),
  {ok, State}.

apply_index_spec([], _Folder) -> 0;

apply_index_spec([{Command, IndexName, IndexValue} | Tail], Folder) ->
  %% lager:error("aplying indexSpec. Command ~p, IndexName ~p, IndexValue ~p", [Command, IndexName, IndexValue]),
  case Command of
    add ->
      add_index(IndexName, IndexValue, Folder);
    remove ->
      remove_index(IndexName, IndexValue, Folder);
    _ ->
      lager:error("Unknown indexSpec command: ~p ", Command)
  end,
  apply_index_spec(Tail, Folder).

add_index(IndexName, _IndexValue, Folder) ->
  IndexFile = filename:join([Folder, "indeces", IndexName]),
  filelib:ensure_dir(filename:join(indexFile, "dummy")),
%%  filelib:is_file
  file:write_file(IndexFile, "to_be_a_link"),
  lager:error("ADD!").

remove_index(_IndexName, _IndexValue, _Folder) ->
  lager:error("REMOVE!").

%% @doc Delete an object from the backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, #state{folder_ref=Folder}=State) ->
  KeyFile = filename:join([Folder, "buckets", Bucket, Key]),
  case file:delete(KeyFile) of
  ok -> {ok, State};
  {error, Reason} -> {error, Reason, State}
  end.

%% @doc Creates a fun that converts entries to string, and then pass it on to FoldFun
list_to_binary_fold_fun(FoldFun) ->
	fun (DataDir, Acc) ->
		StringDataDir = list_to_binary(DataDir),
		FoldFun(StringDataDir, Acc)
	end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [any],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldFun, Acc, _Opts, #state{folder_ref=DataDir}) ->
	RealFun = list_to_binary_fold_fun(FoldFun),
	case file:list_dir(DataDir) of
		{ok, Files} -> AccOut = lists:foldl(RealFun, Acc, Files),
					   {ok, AccOut};
		{error, _Reason} -> {error, "Error listing buckets"}
	end.

lowkey_uber_folder_fun(FoldFun, Bucket) ->
  fun (Key, Acc) ->
    StringKey = filename:basename(list_to_binary(Key)),
    [_|Tail] = string:tokens(binary_to_list(StringKey), "_"),
    case string:equal(Tail, ["content"]) of
      true -> Acc;
      false -> FoldFun(Bucket, StringKey, Acc)
    end
  end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeyFun, Acc, Opts, #state{folder_ref=DataDir}) ->
  Bucket = proplists:get_value(bucket, Opts),
  RealFun = lowkey_uber_folder_fun(FoldKeyFun, Bucket),
  {ok, Files} = file_utils:recursively_list_dir(DataDir, true),
  AccOut = lists:foldl(RealFun, Acc, Files),
    {ok, AccOut}.

list_keys_fold_fun() ->
  fun (KeyFileName, Acc) ->
    [Head|_] = lists:reverse(string:tokens(KeyFileName, "_")),
    case Head of
      "content" -> Acc;
      _ 		-> Acc ++ [filename:basename(KeyFileName)]
    end
  end.

list_keys(DataDir, Bucket) ->
  BucketDataDir = filename:join(DataDir, Bucket),
  case file:list_dir(BucketDataDir) of
    {ok, FileNames} -> lists:foldl(list_keys_fold_fun(), [], FileNames);
    _ -> []
  end.

list_buckets(DataDir) ->
  case file:list_dir(DataDir) of
    {ok, BucketNames} ->  BucketNames;
    _ -> []
  end.

list_buckets(DataDir, undefined) ->
  list_buckets(DataDir);
list_buckets(_DataDir, Bucket) ->
  [Bucket].

fold_objects_over_buckets (FoldFun, DataDir, Buckets, Acc) ->
  lists:foldl(
    fun (Bucket, AccBucket) ->
      Keys = list_keys(DataDir, Bucket),
      lists:foldl(
        fun (Key, AccKey) ->
          KeyFile = filename:join([DataDir, Bucket, Key]),
          case file:read_file(KeyFile) of
            {ok, Value} -> FoldFun(Bucket, Key, Value, AccKey);
            _ -> AccKey
          end
        end
      , AccBucket, Keys)
    end
  , Acc, Buckets).

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{folder_ref=DataDir}) ->
  Bucket = proplists:get_value(bucket, Opts),
  Buckets = list_buckets(DataDir, Bucket),
  fold_objects_over_buckets(FoldObjectsFun, DataDir, Buckets, Acc).

%% @doc Delete all objects from this backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{folder_ref=DataDir}=State) ->
	Buckets = list_buckets(DataDir),
	BucketDeleterFun = 
		fun (Bucket) ->
			Keys = list_keys(DataDir, Bucket),
			ObjectDeleterFun = 
				fun (Key) ->
					KeyPath = filename:join([DataDir, Bucket, Key]),
					file:delete(KeyPath)
				end,
			lists:foreach(ObjectDeleterFun, Keys),
			file:del_dir(filename:join([DataDir, Bucket]))
		end,
	lists:foreach(BucketDeleterFun, Buckets),
	{ok, State}.

%% @doc Returns true if this backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(#state{folder_ref=DataDir}) ->
	{ok, Files} = file_utils:recursively_list_dir(DataDir, true),
	case length(Files) of
		0 -> true;
		_ -> false
	end.

%% @doc Get the status information for this backend
-spec status(state()) -> [{atom(), term()}].
status(_Status) ->
	[{stats, "not too bad"}].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.
