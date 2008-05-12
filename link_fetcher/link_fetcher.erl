%
% Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de).
%

-module(link_fetcher).
-export([start/0]).
-include("uri.hrl").
-include("fetch_manager.hrl").
-record(fetch_state, {root_dir, outstanding_reqs, total_reqs, max_outstanding}).
-define(RATE, 20).

finalize(Req) ->
      UrlName = Req#request.urlname,
      case file:rename(UrlName ++ ".tmp", UrlName) of
          ok ->
              ok;
          Fail ->
              error_logger:error_msg("Could not rename ~s to ~s~n", [UrlName ++ ".tmp", UrlName]),
              Fail
      end.

request_completed(_State, _Req, _Reason, 0) ->
  error_logger:error_msg("Redirect limit reached~n"),
  fail;

request_completed(State, Req, Reason, Retries) ->
  case Reason of
      ok ->
          C = Req#request.callback,
          C(Req);
      {redirect, Location} ->
          spawn(fun() -> sync_post_request(State, redirect_url(Req, Location), Req, Retries) end);
      _ ->
        fail
  end. 

redirect_url(Req, Location) ->
  case Location of
      ("/" ++ _) ->
          "http://" ++ Req#request.host ++ Location;
      ("http://" ++ _) ->
          Location;
      _ ->
      %    %% FIXME: is this really a relative URL?
          "http://" ++ Req#request.host ++ "/" ++ Location
      %     "http://" ++ Location
  end.

sync_post_request(State, URL, Req, Retries) ->
  N = fun(NewReq) ->
    finalize(NewReq),
    file:make_link(NewReq#request.filename, Req#request.filename),
    C = Req#request.callback,
    C(Req)
  end,

  post_request(State, URL, N),
  receive
      {complete, Req2, Reason} ->
          request_completed(State, Req2, Reason, Retries-1)
  after 10000 ->
      State
  end.

loop(StateO) ->
    State = cleanup(StateO),
    case next_line() of
        {ok, Line} ->
            Y = post_request(State, my_utils:chomp(Line), fun(C) -> finalize(C) end),
            Total = State#fetch_state.total_reqs,
            my_utils:rate_error_logger("Total: ~p~n", Total, ?RATE),
            NextState = State#fetch_state{
                outstanding_reqs = State#fetch_state.outstanding_reqs + Y,
                total_reqs = Total + 1},
            loop(NextState);
        _ ->
            State
    end.

post_request(State, URL, Callback) ->
    case uri:parse(URL) of
        #http_uri{host=Host, port=_Port}=HttpUri ->
            Filename = uri:to_filename(HttpUri, State#fetch_state.root_dir),
            UrlFilename = Filename ++ ".url",
            BodyFilename = Filename ++ ".data",
            Uri = uri:normalize_to_s(HttpUri), 
          
            case filelib:is_file(UrlFilename) orelse filelib:is_file(UrlFilename ++ ".tmp") of
                true ->
                    0;
                false ->
                    filelib:ensure_dir(Filename),
                    file:write_file(UrlFilename ++ ".tmp", Uri ++ "\n"),
                    case my_utils:resolve_host(Host) of
                        error ->
                            error_logger:error_msg("DNS resolv failed: ~p~n", [Host]),
                            0;
                        IP ->
                            R = #request{
                                requestor_pid = self(),
                                server_ip = IP,
                                port = HttpUri#http_uri.port,
                                host = HttpUri#http_uri.host,
                                request_uri = uri:request_uri(HttpUri),
                                filename = BodyFilename,
                                urlname = UrlFilename,
                                callback = Callback},

			    fetch_manager:post_request(whereis(fetcher), R),
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

    State = loop(initial_state()),
    error_logger:info_msg("Finish: ~p~n", [State#fetch_state.outstanding_reqs]),
    finish(State),
    error_logger:info_msg("Finished~n").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cleanup(#fetch_state{outstanding_reqs=O, max_outstanding=M}=State) when (O < M) ->
    receive
        {complete, Req, Reason} ->
            request_completed(State, Req, Reason, 3),
            cleanup(State#fetch_state{outstanding_reqs = O - 1})
    after 0 ->
        State
    end;
cleanup(#fetch_state{outstanding_reqs=O, max_outstanding=M}=State) when (O >= M) ->
    receive
        {complete, Req, Reason} ->
            request_completed(State, Req, Reason, 3),
            cleanup(State#fetch_state{outstanding_reqs = O - 1})
    after 10000 ->
      error_logger:info_msg("Stuck in cleanup! 10 seconds no completion!~n"),
      cleanup(State)
    end.

finish(#fetch_state{outstanding_reqs=0}=State) -> State;
finish(#fetch_state{outstanding_reqs=O}=State) when (O > 0) ->
    my_utils:rate_error_logger("Finish (~p)~n", O, ?RATE),
    receive
        {complete, Req, Reason} ->
            request_completed(State, Req, Reason, 3),
            finish(State#fetch_state{outstanding_reqs = O - 1})
    after 10000 ->
      error_logger:info_msg("Stuck in finish! 10 seconds no completion!~n"),
      finish(State)
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
