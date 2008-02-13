-module(submit).
-export([start/0]).
-include("defs.hrl").
-define(URL_FILE, "/home/mneumann/adbrite-urls.txt").

start() ->
    setup_node:start("error.log"),
    Workers = distribute:get_workers(?NODES),
    {ok, F} = file:open(?URL_FILE, [read]),
    lists:map(fun worker:bulk_start/1, Workers),

    my_utils:each_line_with_index(F, fun(L, I) -> 
        io:format("Line ~p~n", [I]),
        L2 = my_utils:chomp(L),
        case distribute:submit(L2, Workers, fun(Entry, Pid) ->
          worker:bulk_submit(Entry, Pid) end) of
            ok -> ok;
            Res ->
                error_logger:error_msg("submit of ~p failed with ~p~n", [L2, Res]) 
        end
    end),

    lists:map(fun worker:bulk_stop/1, Workers).
