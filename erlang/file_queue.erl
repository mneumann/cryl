%
% File Queue implements a queuing mechanism that
% is based on multiple queue files organized in a
% "current" and "next" generation.
%
% An enqueue operation takes place in the "next" 
% generation, a dequeue in the "current".
% Once the "current" queue is empty, it's exchanged
% with the "next" generation.
%
% Each "next" and "current" consists of a number
% files. Once a file in the "current" queue reaches
% the end, it is removed completely from the "current"
% queue.  
%
% The advantages are two fold:
%
%   1) Provides more fine-grained fall-back in case of
%      a shutdown.
%
%   2) Provides randomization of execution order, by simply 
%      enqueuing into a random file.
%
% Each line in a queue file is an entry.
%
% Note: Insert order is NOT preserved!
% 
% Copyright (c) 2008 by Michael Neumann
%
-module(file_queue).
-record(file_queue, {num_queues, root_dir, active_queue, active_queue_name}).
-export([create/2, open/1, enqueue/2, dequeue/1, enq/2, deq/1]).

%
% Creates initial directory structure and files
% for a File Queue. 
%
create(QueueRoot, NumQueues) ->
    filelib:ensure_dir(QueueRoot),
    file:make_dir(QueueRoot),
    file:make_dir(filename:join(QueueRoot, "current")),
    file:make_dir(filename:join(QueueRoot, "next")),
    file:write_file(filename:join(QueueRoot, "iter"), "1"),
    file:write_file(filename:join(QueueRoot, "numq"), integer_to_list(NumQueues)),
    #file_queue{num_queues = NumQueues, root_dir = QueueRoot}.

%
% Expects an existing directory structure
% for a File Queue. 
%
open(QueueRoot) ->
    {ok, Bin} = file:read_file(filename:join(QueueRoot, "numq")),
    NumQueues = list_to_integer(binary_to_list(Bin)),
    #file_queue{num_queues = NumQueues, root_dir = QueueRoot}.

enqueue(Entry, FileQueue) ->
    QueueNo  = random:uniform(FileQueue#file_queue.num_queues - 1), 
    Filename = filename:join([FileQueue#file_queue.root_dir, "next", 
        integer_to_list(QueueNo)]),
    case {string:chr(Entry, 13), string:chr(Entry, 10)} of
        {0,0} ->
            file:write_file(Filename, Entry ++ "\n", [append]),
            ok;
        _ -> invalid_entry
    end.

queue_empty(FileQueue, Queue) ->
    case queue_files(FileQueue, Queue) of
        [] ->
            true;
        _ ->
            false
    end.

queue_name(FileQueue, Queue) ->
    filename:join(FileQueue#file_queue.root_dir, Queue).

queue_files(FileQueue, Queue) ->
    case file:list_dir(queue_name(FileQueue, Queue)) of
        {ok, List} ->
            List;
        _ ->
            []
    end.

open_queue_file(_FileQueue, _Queue, []) ->
    failed;

open_queue_file(FileQueue, Queue, [Name|T]) ->
    FullName = filename:join([FileQueue#file_queue.root_dir, Queue, Name]),
    case file:open(FullName, [read]) of
        {ok, F} ->
            {FullName, F};
        _ ->
            error_logger:error_msg("Coudn't open queue file ~p. Delete anyway.~n", [FullName]),
            file:delete(FullName),
            open_queue_file(FileQueue, Queue, T)
    end.
 
switch_queues(FileQueue) ->
    A = queue_name(FileQueue, "current"),
    B = queue_name(FileQueue, "current.tmp"),
    C = queue_name(FileQueue, "next"),
    ok = file:rename(A, B),
    ok = file:rename(C, A),
    ok = file:rename(B, C),
    ok.

dequeue(FileQueue) ->
    case FileQueue#file_queue.active_queue of
        undefined ->
            case queue_files(FileQueue, "current") of
                [] ->
                    % "current" queue is empty.
                    % if "next" is empty as well, return empty. 
                    case queue_empty(FileQueue, "next") of
                        true ->
                            undefined;
                        false ->
                            switch_queues(FileQueue),
                            dequeue(FileQueue)
                    end;
                List -> 
                    case open_queue_file(FileQueue, "current", List) of
                        failed ->
                            case queue_empty(FileQueue, "next") of
                                true ->
                                    undefined;
                                false ->
                                    switch_queues(FileQueue),
                                    dequeue(FileQueue)
                            end;
                        {FullName, F} ->
                            dequeue(FileQueue#file_queue{
                                active_queue = F,
                                active_queue_name = FullName})
                    end
            end;
        F ->
            case io:get_line(F, '') of
                eof ->
                    file:delete(FileQueue#file_queue.active_queue_name),
                    dequeue(FileQueue#file_queue{active_queue=undefined,
                                                 active_queue_name=undefined});
                Line ->
                    {my_utils:chomp(Line), FileQueue}
            end
    end.

%
% Aliases for enqueue/dequeue.
%
enq(Entry, FileQueue) -> enqueue(Entry, FileQueue).
deq(FileQueue) -> dequeue(FileQueue).
