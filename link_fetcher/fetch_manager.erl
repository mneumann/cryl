%
% The fetch manager maintains a number of fetch processes
% and also takes care that not more than one connection 
% to an IP address is established. 
% 
% Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
%
-module(fetch_manager).
-export([start/1, post_request/2]).

-include("uri.hrl").
-include("fetch_manager.hrl").
-record(state, {connections, active_requests, queued_requests, max_conns}).

start(MaxConns) ->
    process_flag(trap_exit, true),
    loop(initial_state(MaxConns)).

initial_state(MaxConns) ->
    #state{
        connections = gb_sets:new(),
        active_requests = gb_trees:empty(), 
        queued_requests = gb_trees:empty(),
        max_conns = MaxConns }.

post_request(FetcherPid, Req) ->
    FetcherPid ! {req, Req}.

loop(State) ->
    receive
        {req, R} ->
            case is_request_busy(R, State) of
                true ->
                    % enqueue and continue
                    loop(enqueue_request(R, State));
                false ->
                    % start request
                    loop(start_request(R, State))
            end;
        {'EXIT', Pid, Reason} ->
            loop(complete_request(Pid, Reason, State))
    end.

%
% Complete the active request handled by PID. 
%
complete_request(Pid, Reason, State) ->
    {Request, State2} = remove_active_request(Pid, State),
   
    %
    % Lets see if there is a request with the same IP as 
    % the completing request in the queue.
    % If we find such a request, we start it.
    %
    {NextRequest, State3} = next_request_from_queue(Request, State2),
    State4 = start_request(NextRequest, State3),
    notify_requestor(Request, Reason),
    State4.

%
% Notify Requestor that the request completed 
%
notify_requestor(Request, Reason) ->
    Request#request.requestor_pid ! {complete, Request, Reason}.

%
% Looks for a request with the same server_ip as Request
% and removes it from the queue.
%
% Returns {NextRequest, NewState} or
%         {none, NewState}
%
next_request_from_queue(Request, State) ->
    Key = Request#request.server_ip,
    Q   = State#state.queued_requests,
    {NextRequest, NQ} =
    case gb_trees:lookup(Key, Q) of
        {value, []} ->
            {none, gb_trees:delete(Key, Q)};
        {value, [R]} ->
            {R, gb_trees:delete(Key, Q)};
        {value, [R|T]} ->
            {R, gb_trees_replace(Key, T, Q)};
        none ->
            {none, Q}
    end,
    {NextRequest, State#state{queued_requests = NQ}}.

%
% Remove the active request indexed by the
% handling process 'Pid'.
%
% Returns {Request, State}.
%
remove_active_request(Pid, State) ->
    Request = gb_trees:get(Pid, State#state.active_requests),
    {Request,
     State#state{    
       active_requests = gb_trees:delete(Pid, State#state.active_requests),
       connections = gb_sets:delete(Request#request.server_ip, State#state.connections)
     }}.

%
% Start Request.
% Returns updated State.
%
start_request(none, State) -> State;
start_request(Request, State) ->
    Pid = spawn_link(fun() ->
        Res = http_client:download({
            Request#request.server_ip,
            Request#request.port,
            Request#request.host,
            Request#request.request_uri},
            Request#request.filename),
        % Notify the linked process with the return status.
        exit(Res)
    end),
    State#state{
        connections =
            gb_sets:insert(Request#request.server_ip, State#state.connections),
        active_requests =
            gb_trees:insert(Pid, Request, State#state.active_requests)
    }.

%
% Enqueue Request into the request queue.
% Returns updated State.
%
enqueue_request(Request, State) ->
    Key = Request#request.server_ip,
    Q = State#state.queued_requests,
    case gb_trees:lookup(Key, Q) of
        {value, List} ->
            % We prepend new requests because that's faster. 
            State#state{queued_requests =
                            gb_trees_replace(Key, [Request|List], Q)};
        none -> 
            State#state{queued_requests =
                            gb_trees:insert(Key, [Request], Q)}
    end.

%
% Tests whether Request is busy (i.e. it can't be issued).
%
% Busy ::= "Max number of connections reached" or
%          "Connection to same IP exists" 
%
is_request_busy(Request, State) ->
    (gb_sets:size(State#state.connections) >= State#state.max_conns) orelse
    gb_sets:is_element(Request#request.server_ip, State#state.connections).

%
% Helper function
%
gb_trees_replace(Key, Value, T) ->
    T1 = gb_trees:delete(Key, T),
    gb_trees:insert(Key, Value, T1).
