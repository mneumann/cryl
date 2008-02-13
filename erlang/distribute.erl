-module(distribute).
-include("uri.hrl").
-export([distribute/2, submit/2, submit/3, get_worker/1, get_workers/1]).

distribute(_HttpUri, []) ->
    {error, no_nodes};
distribute(HttpUri, ListOfNodes) ->
    case HttpUri#http_uri.host_tokrl of
        [] ->
            {error, empty_host};
        [_H] ->
            {error, host_to_short};
        [_H|T] ->
            % use all except the last, to distribute to a node.
            % as list is already reversed, we just take T.
            N = erlang:phash(T, length(ListOfNodes)),
            {ok, lists:nth(N, ListOfNodes)}
    end.

get_worker(Node) ->
    rpc:call(Node, erlang, whereis, [worker]).

get_workers(ListOfNodes) ->
    lists:map(fun get_worker/1, ListOfNodes).

submit(Entry, ListOfWorkers) ->
    submit(Entry, ListOfWorkers, fun(Entry, Pid) -> worker:submit(Entry, Pid) end).

submit(Entry, ListOfWorkers, Fun) ->
    case uri:parse(Entry) of
        #http_uri{} = HttpUri ->
            case distribute(HttpUri, ListOfWorkers) of
                {ok, WorkerPid} ->
                    Fun(Entry, WorkerPid);
                Err ->
                    Err
            end;
        Err ->
            Err
    end.
