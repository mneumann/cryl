%
% Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de).
%

-module(link_fetcher).
-export([start/0]).
-include("./uri.hrl").
-record(fetch_state, {root_dir, outstanding_reqs, total_reqs, max_outstanding}).

request_completed(_Req, _Reason) -> ok.

loop(StateO) ->
    State = cleanup(StateO),
    case next_line() of
        {ok, Line} ->
            Y = post_request(State, my_utils:chomp(Line)),
            Total = State#fetch_state.total_reqs,
            case (Total rem 100) of
                0 ->
                    error_logger:info_msg("Total: ~p~n", [Total]);
                _ ->
                    ok
            end,

            NextState = State#fetch_state{
                outstanding_reqs = State#fetch_state.outstanding_reqs + Y,
                total_reqs = Total + 1},
            loop(NextState);
        _ ->
            error_logger:info_msg("Finish: ~p~n", [State#fetch_state.outstanding_reqs]),
            finish(State)
    end.

post_request(State, URL) ->
    case uri:parse(URL) of
        #http_uri{host=Host, port=Port}=HttpUri ->
            Filename = uri:to_filename(HttpUri, State#fetch_state.root_dir),
            UrlFilename = Filename ++ ".url",
            BodyFilename = Filename ++ ".data",
            Uri = uri:normalize_to_s(HttpUri), 
          
            case filelib:is_file(UrlFilename) of
                true ->
                    error_logger:info_msg("Skip already fetched URL: ~s~n", [Uri]),
                    0;
                false ->
                    filelib:ensure_dir(Filename),
                    file:write_file(UrlFilename, Uri ++ "\n"),
                    case my_utils:resolve_host(Host) of
                        error ->
                            error_logger:error_msg("DNS resolv failed: ~p~n", [Host]),
                            0;
                        IP ->
			    fetch_manager:post_request(whereis(fetcher), IP, HttpUri, BodyFilename),
                            1
                    end
            end;
        {error, _} ->
            error_logger:error_msg("Invalid URL: ~p~n", [URL]),
            0
    end.

initial_state() ->
    #fetch_state{
        root_dir = settings:root_dir(), 
        outstanding_reqs = 0,
        total_reqs = 0,
        max_outstanding = settings:max_outstanding()}.


start() ->
    crypto:start(),

    case settings:error_log() of
        false ->
            error_logger:tty(true);
        LogFile ->
            error_logger:logfile({open, LogFile}),
            error_logger:tty(false)
    end,

    register(fetcher, 
        spawn(fun() -> fetch_manager:start(settings:max_conns()) end)),

    loop(initial_state()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cleanup(#fetch_state{outstanding_reqs=O, max_outstanding=M}=State) when (O < M) ->
    receive
        {complete, Req, Reason} ->
            request_completed(Req, Reason),
            cleanup(State#fetch_state{outstanding_reqs = O - 1})
    after 0 ->
        State
    end;
cleanup(#fetch_state{outstanding_reqs=O, max_outstanding=M}=State) when (O >= M) ->
    receive
        {complete, Req, Reason} ->
            request_completed(Req, Reason),
            cleanup(State#fetch_state{outstanding_reqs = O - 1})
    end.

finish(#fetch_state{outstanding_reqs=0}=State) -> State;
finish(#fetch_state{outstanding_reqs=O}=State) when (O > 0) ->
    receive
        {complete, Req, Reason} ->
            request_completed(Req, Reason),
            finish(State#fetch_state{outstanding_reqs = O - 1})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

next_line() ->
    case io:get_line('') of
        Str when is_list(Str) ->
            {ok, Str};
        eof ->
            eof;
        _ ->
            error_logger:error_msg("get_line failed~n"),
            {error, 'get_line failed'}
    end.
