-module(main).
-export([create/0, start/0]).

create() ->
    worker:create("./work", 213, 1000, 10000).  

start() ->
    crypto:start(),
    error_logger:logfile({open, "error.log"}),
    error_logger:tty(false),
    Worker = worker:open_and_start("./work"),
    {ok, F} = file:open("/home/mneumann/adbrite-urls.txt", [read]),
    my_utils:each_line(F, fun(L) -> 
        L2 = my_utils:chomp(L),
        case worker:submit(L2, Worker) of
            ok -> ok;
            Res ->
                error_logger:error_msg("submit of ~p failed with ~p~n", [L2, Res]) 
        end
    end, 100).
