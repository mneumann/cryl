-module(main).
-export([create/0, start/0]).

create() ->
    worker:create("./work", 61, 1000, 10000).  

start() ->
    crypto:start(),
    Worker = worker:open_and_start("./work"),
    {ok, F} = file:open("/home/mneumann/adbrite-urls.txt", [read]),
    my_utils:each_line(F, fun(L) -> 
        L2 = my_utils:chomp(L),
        R = worker:submit(L2, Worker),
        io:format("R = ~p~n", [R])
    end, 10000),
    io:format("DONE~n").
