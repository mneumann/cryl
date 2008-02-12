-module(test_file_queue).
-export([start/0]).

start() ->
    FQ = file_queue:create("./work", 213),
    file_queue:enq("Test", FQ),
    file_queue:enq("Test2", FQ),
    file_queue:enq("Test3", FQ),
    file_queue:enq("abc", FQ),
    file_queue:enq("1123232", FQ),
    read_till_empty(FQ).

read_till_empty(FQ) ->
    case file_queue:deq(FQ) of
        {Line, NFQ} ->
            io:format("~p~n", [Line]),
            read_till_empty(NFQ);
        undefined ->
            io:format("QUEUE EMPTY~n")
    end.
