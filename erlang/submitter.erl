-module(submitter).
-export([start/2]).

start(Nodes, UrlFile) ->
    setup_node:start("error.log"),
    Workers = distribute:get_workers(Nodes),
    {ok, F} = file:open(UrlFile, [read]),

    my_utils:each_line_with_index(F, fun(L, I) -> 
        io:format("Line ~p~n", [I]),
        L2 = my_utils:chomp(L),
        case distribute:submit(L2, Workers) of
            ok -> ok;
            Res ->
                error_logger:error_msg("submit of ~p failed with ~p~n", [L2, Res]) 
        end
    end).
