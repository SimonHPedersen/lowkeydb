
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

-record(state, {basedir_ref :: string()}).

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
  BaseDir = app_helper:get_prop_or_env(data_root, Config, lowkeydb),
  PartitionRootDir = filename:join([BaseDir, integer_to_list(Partition)]),
  PartitionDataDir = filename:join(PartitionRootDir, "buckets"),
  PartitionIndexDir = filename:join(PartitionRootDir, "indeces"),
  filelib:ensure_dir(filename:join(PartitionDataDir, "dummy")),
  filelib:ensure_dir(filename:join(PartitionIndexDir, "dummy")),
  {ok, #state{basedir_ref=PartitionRootDir}}.

%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(_State) ->
    ok.

%% @doc Retrieve an object from the backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{basedir_ref=BaseDir}=State) ->
  KeyFile = filename:join([BaseDir, "buckets", Bucket, Key]),
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

put(Bucket,  Key, IndexSpec, Value, #state{basedir_ref=BaseDir}=State) ->
  RawFile = filename:join([BaseDir, "buckets", Bucket, Key]),
  filelib:ensure_dir(RawFile),
  ContentFile = filename:join([BaseDir, Bucket, string:concat(binary_to_list(Key), "_content")]),
  [ContentTerm] = element(4, binary_to_term(Value)), %% unpack list by using []
  ContentAsIntArray = binary_to_list(element(3, ContentTerm)),
  file:write_file(RawFile, Value),
  file:write_file(ContentFile, ContentAsIntArray),

  %% update indeces
  apply_index_spec(IndexSpec, BaseDir, Bucket, Key),
  {ok, State}.

apply_index_spec([], _BaseDir, _Bucket, _Key) -> 0;

apply_index_spec([{Command, IndexName, IndexValue} | Tail], BaseDir, Bucket, Key) ->
%% lager:error("aplying indexSpec. Command ~p, IndexName ~p, IndexValue ~p", [Command, IndexName, IndexValue]),
  case Command of
    add ->
      add_index(IndexName, IndexValue, BaseDir, Bucket, Key);
    remove ->
      remove_index(IndexName, IndexValue, BaseDir, Bucket, Key);
    _ ->
      lager:error("Unknown indexSpec command: ~p ", Command)
  end,
  apply_index_spec(Tail, BaseDir, Bucket, Key).

add_index(IndexName, IndexValue, BaseDir, Bucket, Key) ->
  IndexFile = filename:join([BaseDir, "indeces", IndexName, IndexValue, Bucket, Key]),
  filelib:ensure_dir(IndexFile),
  TargetFile = filename:join(["../../../../buckets", Bucket, Key]),
  lager:error("Adding symlink from ~p to ~p", [IndexFile, TargetFile]),
  file:make_symlink(TargetFile, IndexFile).

remove_index(IndexName, IndexValue, BaseDir, Bucket, Key) ->
  IndexFile = filename:join([BaseDir, "indeces", IndexName, IndexValue, Bucket, Key]),
  lager:error("Deleting IndexFile ~p", [IndexFile]),
  file:delete(IndexFile).


%% @doc Delete an object from the backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, #state{basedir_ref=BaseDir}=State) ->
  KeyFile = filename:join([BaseDir, "buckets", Bucket, Key]),
  case file:delete(KeyFile) of
  ok -> {ok, State};
  {error, Reason} -> {error, Reason, State}
  end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [any],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldFun, Acc, _Opts, #state{basedir_ref=BaseDir}) ->
  BucketDir = filename:join(BaseDir, "buckets"),
	RealFun = list_to_binary_fold_fun(FoldFun),
	case file:list_dir(BucketDir) of
		{ok, Files} -> AccOut = lists:foldl(RealFun, Acc, Files),
					   {ok, AccOut};
		{error, _Reason} -> {error, "Error listing buckets"}
	end.

%% @doc Creates a fun that converts entries to string, and then pass it on to FoldFun
list_to_binary_fold_fun(FoldFun) ->
  fun (BaseDir, Acc) ->
    StringBaseDir = list_to_binary(BaseDir),
    FoldFun(StringBaseDir, Acc)
  end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeyFun, Acc, Opts, #state{basedir_ref=BaseDir}) ->
  case Opts of
    [{index, IndexBucket, IndexLookup},_] ->
      lager:error("fold_keys. IndexBucket ~p IndexLookup ~p", [IndexBucket,IndexLookup]),
    case IndexLookup of
      {range, IndexName, StartKey, EndKey} ->
        lager:error("fold_keys. range search. Index: ~p StartKey: ~p EndKey: ~p", [IndexName, StartKey, EndKey]),
        case IndexName of
          <<"$key">> ->
            lager:error("range lookup on special $key index"),
            BucketDir = filename:join([BaseDir, "buckets", IndexBucket]),
            case file:list_dir(BucketDir) of
              {ok, AllFilesInBucket} ->
                lager:error("found files doing range lookup on $key ~p", [AllFilesInBucket]),
                RealFun = key_filter_folder_fun(FoldKeyFun, IndexBucket, StartKey, EndKey),
                {ok, lists:foldl(RealFun, Acc, AllFilesInBucket)};
                _ -> {ok, Acc}
            end;
          _ ->
                IndexDir = filename:join([BaseDir, "indeces", IndexName]),
            case file:list_dir(IndexDir) of
              {ok, AllIndexValueDirs} ->
                lager:error("AllIndexValueDirs: ~p", AllIndexValueDirs),
                RealFun = indexValue_filtering_folder_fun(FoldKeyFun, IndexDir, StartKey, EndKey),
                AccOut = lists:foldl(RealFun, Acc, AllIndexValueDirs),
                {ok, AccOut};
              _ -> {ok, Acc}
            end
          end;

        {eq, IndexName, IndexValue} ->
        lager:error("fold_keys. Direct index lookup. IndexName: ~p IndexValue: ~p", [IndexName, IndexValue]),

        case IndexName of
          <<"$key">> ->
            lager:error("direct lookup on special $key index"),
            FileName = filename:join([BaseDir, "buckets", IndexBucket, IndexValue]),
            case filelib:is_file(FileName) of
              true ->
                {ok, FoldKeyFun(IndexBucket, IndexValue, Acc)};
              false ->
                {ok, Acc}
            end;
            _ ->
            IndexDir = filename:join([BaseDir, "indeces", IndexName, IndexValue]),
            case filelib:is_dir(IndexDir) of
              true ->
                {ok, Files} = file_utils:recursively_list_dir(IndexDir, true),
                lager:error("fold_keys. found Files: ~p", [Files]),
                RealFun = filename_2_bucket_key_folder_fun(FoldKeyFun),
                AccOut = lists:foldl(RealFun, Acc, Files),
                {ok, AccOut};
              false ->    %% Nothing found
                {ok, Acc}
            end
        end
    end;
    [{bucket, Bucket}] ->
      lager:error("fold_keys. Listing all keys in Bucket ~p", [Bucket]),
      BucketDir = filename:join(BaseDir, "buckets"),
      {ok, Files} = file_utils:recursively_list_dir(BucketDir, true),
      RealFun = lowkey_uber_folder_fun(FoldKeyFun),
      AccOut = lists:foldl(RealFun, Acc, Files),
      {ok, AccOut}
  end.

lowkey_uber_folder_fun(FoldFun) ->
  fun (Key, Acc) ->
    StringKey = filename:basename(list_to_binary(Key)),
    [_|Tail] = string:tokens(binary_to_list(StringKey), "_"),
    case string:equal(Tail, ["content"]) of
      true -> Acc;
      false -> FoldFun(StringKey, Acc)
    end
  end.

filename_2_bucket_key_folder_fun(FoldFun) ->
  fun (FileName, Acc) ->
    Tokens = filename:split(FileName),
    Key = lists:last(Tokens),
    Bucket = lists:nth(length(Tokens) -1, Tokens),
    lager:error("Bucket ~p Key ~p", [Bucket, Key]),
    FoldFun(Bucket, Key, Acc)
  end.

indexValue_filtering_folder_fun(FoldFun, IndexNameDir, StartKey, EndKey) ->
  fun (IndexValueDir, Acc) ->
    IndexValueDirBinary = list_to_binary(IndexValueDir),
    lager:error("IndexValueDir: ~p, StartKey: ~p, EndKey: ~p", [IndexValueDirBinary, StartKey, EndKey]),
    case StartKey =< IndexValueDirBinary andalso EndKey >= IndexValueDirBinary of
      true ->
        lager:error("In range"),
        case file_utils:recursively_list_dir(filename:join(binary_to_list(IndexNameDir), IndexValueDir), true) of
          {ok, Files} ->
          lager:error("Found files: ~p", [Files]),
          RealFun = file_2_bucket_key_folder_fun(FoldFun),
          AccOut = lists:foldl(RealFun, Acc, Files),
          lager:error("Done"),
          AccOut
        end;
      false ->
        lager:error("Outside range. Ignore"),
        Acc
    end
end.

file_2_bucket_key_folder_fun(FoldFun) ->
  fun (FileName, Acc) ->
    Tokens = filename:split(FileName),
    Key = lists:last(Tokens),
    Bucket = lists:nth(length(Tokens) -1, Tokens),
    lager:error("IIIIBucket ~p Key ~p", [Bucket, Key]),
    FoldFun(list_to_binary(Bucket), list_to_binary(Key), Acc)
  end.

key_filter_folder_fun(FoldFun, Bucket, StartKey, EndKey) ->
  fun (Key, Acc) ->
    KeyBin = list_to_binary(Key),
    lager:error("file_2_bucket_key_folder_fun2 Bucket ~p Key ~p, StartKey ~p, EndKey ~p", [Bucket, KeyBin, StartKey, EndKey]),
    case KeyBin >= StartKey andalso KeyBin =< EndKey of
      true ->
        FoldFun(Bucket, KeyBin , Acc);
      false ->
        lager:error("Outside range. Ignore"),
        Acc
    end
  end.


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
fold_objects(FoldObjectsFun, Acc, Opts, #state{basedir_ref=DataDir}) ->
  Bucket = proplists:get_value(bucket, Opts),
  Buckets = list_buckets(DataDir, Bucket),
  fold_objects_over_buckets(FoldObjectsFun, DataDir, Buckets, Acc).

%% @doc Delete all objects from this backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{basedir_ref=DataDir}=State) ->
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
is_empty(#state{basedir_ref=DataDir}) ->
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
