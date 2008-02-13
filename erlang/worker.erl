%
% A worker has it's own file queue and is responsible for a part of the
% whole domain space.
%
% It accepts urls received as message and puts them into the queue.
%
% Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de).
%

-module(worker).
-export([create/4, open/1, open_and_start/1, start/1, submit/2]).
-record(worker, {file_queue, max_conns, max_outstanding, outstanding,
                 fetch_manager_pid, worker_pid, root_dir}).
-define(WAIT_INF, 1000000). % 1000 seconds
-include("uri.hrl").

create(RootDir, NumQueues, MaxConns, MaxOutstanding) ->
    case filelib:is_file(RootDir) of
        true ->
            {error, root_dir_exists};
        false ->
            FQ = file_queue:create(RootDir, NumQueues),
            file:make_dir(filename:join(RootDir, "files")),
            file:write_file(filename:join(RootDir, "maxconn"), integer_to_list(MaxConns)),
            file:write_file(filename:join(RootDir, "maxouts"), integer_to_list(MaxOutstanding)),
            #worker{file_queue = FQ, max_conns = MaxConns,
                    max_outstanding = MaxOutstanding, root_dir = RootDir,
                    outstanding = 0}
    end.

open(RootDir) ->
    {ok, Bin} = file:read_file(filename:join(RootDir, "maxconn")),
    {ok, Bin2} = file:read_file(filename:join(RootDir, "maxouts")),
    #worker{file_queue = file_queue:open(RootDir),
            max_conns = list_to_integer(binary_to_list(Bin)),
            max_outstanding = list_to_integer(binary_to_list(Bin2)),
            root_dir = RootDir,
            outstanding = 0}.

%
% Start fetch manager and worker processes.
% TODO: link, add fault tolerance.
%
start(Worker) ->
    FetcherPid = spawn(fun() -> fetch_manager:start(Worker#worker.max_conns) end),
    W = Worker#worker{fetch_manager_pid = FetcherPid},
    Pid = spawn(fun() -> loop(W) end),
    W#worker{worker_pid = Pid}.

open_and_start(RootDir) ->
    start(open(RootDir)).
 
submit(Entry, #worker{worker_pid = Pid}) ->
    submit(Entry, Pid);
submit(Entry, Pid) ->
    Pid ! {req, self(), Entry},
    receive
        {req_confirm, Pid, Entry, Reason} -> Reason
    end.

%--------------------------------------------------------------------
%--------------------------------------------------------------------

%
% File queue is empty. Wait for requests to leave this state.
%
loop_empty_queue(#worker{outstanding = Outst} = Worker) ->
    case check_messages(Worker, ?WAIT_INF) of
        -1 ->
            % request completed, but we can't a post a new request as
            % the file queue is empty.
            loop_empty_queue(Worker#worker{outstanding = Outst - 1});
        0 ->
            % nothing happened (timeout)
            loop_empty_queue(Worker);
        1 ->
            % request enqueued into file queue
            loop(Worker)
    end.

loop(#worker{outstanding = Outst, max_outstanding = MaxOutst} = Worker) ->
    OutsBalance =
    case (Outst >= MaxOutst) of
        true ->
            check_messages(Worker, ?WAIT_INF);
        false ->
            check_messages(Worker, 0)
    end,

    NewOutst = case OutsBalance of
        -1 -> Outst - 1;
        _ -> Outst
    end,

    case (NewOutst >= MaxOutst) of
        true ->
            loop(Worker#worker{outstanding = NewOutst});
        false ->
            case file_queue:dequeue(Worker#worker.file_queue) of
                undefined ->
                    % wait queue seems to be empty
                    loop_empty_queue(Worker#worker{outstanding = NewOutst});
                {Entry, NewFQ} ->
                    NewOutst2 = NewOutst + post_request(Worker, Entry),
                    loop(Worker#worker{outstanding = NewOutst2, file_queue = NewFQ})
                    % TODO: put "Entry" into in-memory active_queue, to not
                    % loose it when shutting down the worker.
            end
    end.

%
% Returns 0, 1 or -1, depending on whether a request has been completed
% (-1), enqueued (1) or nothing happened (0).
%
check_messages(Worker, Wait) -> 
    receive
        {req, Pid, Entry} ->
            % receive a request from a submitter and put it into
            % the file queue, then confirm that the request has
            % been put into the queue by replying with a message
            % to the submitter.
            Ret = file_queue:enqueue(Entry, Worker#worker.file_queue),
            Pid ! {req_confirm, self(), Entry, Ret},
            1;
        {complete, Req, Reason} ->
            % receive a completion message from the fetch manager.
            % TODO: include Pid of fetcher in the message.
            error_logger:info_msg("Request ~p completed with ~p~n", [Req, Reason]),
            -1
    after Wait ->
        0
    end.

%
% Tries to start a fetch request. Returns the balance of started fetch
% requests, i.e. either 0 or 1.
%
post_request(Worker, UrlStr) ->
    URL = my_utils:strip(UrlStr),
    case uri:parse(URL) of
        #http_uri{host = Host}=HttpUri ->
            FileRoot = filename:join(Worker#worker.root_dir, "files"),
            Filename = uri:to_filename(HttpUri, FileRoot),
            case filelib:is_file(Filename) of
                true ->
                    % file already exists. skip it
                    error_logger:info_msg("Skip file ~p~n", [Filename]),
                    0;
                false ->
                    filelib:ensure_dir(Filename),
                    % create the file to make sure that it isn't 
                    % fetched again in case of an error. FIXME.
                    file:write_file(Filename, ""),
                    case my_utils:resolve_host(Host) of
                        error ->
                            error_logger:error_msg("DNS resolve failed for ~p~n", [Host]),
                            0;
                        IP ->
                            fetch_manager:post_request(Worker#worker.fetch_manager_pid,
                                                       IP, HttpUri, Filename),
                            1
                    end
            end;
        _ ->
            error_logger:error_msg("Invalid URL ~p~n", [URL]),
            0
    end.
