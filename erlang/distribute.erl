-module(distribute).
-include("uri.hrl").
-export([distribute/2]).

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
