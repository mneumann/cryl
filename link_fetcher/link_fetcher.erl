%
% Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de).
%

-module(link_fetcher).
-export([start/0]).
-include("./uri.hrl").
-define(ROOT_DIR, "./work").

request_completed(_Req, _Reason) -> ok.

try_cleanup(Sum) ->
    receive
        {complete, Req, Reason} ->
            request_completed(Req, Reason),
            try_cleanup(Sum+1)
    after 0 ->
        Sum
    end.
try_cleanup() -> try_cleanup(0).

cleanup(0, Sum) -> Sum;
cleanup(Outstanding, Sum) ->
    io:format("cleanup[~p]~n", [Outstanding]),
    receive
        {complete, Req, Reason} ->
            request_completed(Req, Reason),
            cleanup(Outstanding-1, Sum+1)
    end.

cleanup(N) -> cleanup(N, 0).

loop(LineNo, 0=Avail) -> 
    loop(LineNo, Avail+cleanup(1));

loop(LineNo, Avail) ->
    Avail2 = Avail + try_cleanup(),
    case io:get_line('') of
        Str when is_list(Str) ->
	    io:format("Line: ~p~n", [LineNo]),
            Y = post_request(my_utils:strip(Str)),
            loop(LineNo+1, Avail2+Y);
        eof ->
            cleanup(settings:max_outstanding() - Avail);
        _ ->
            io:format("ERROR get_line~n"),
            cleanup(settings:max_outstanding() - Avail)
    end.

post_request(URL) ->
    case uri:parse(URL) of
        #http_uri{host=Host, port=Port}=HttpUri ->
            Filename = uri:to_filename(HttpUri, ?ROOT_DIR),
            case filelib:is_file(Filename) of
                true ->
                    % file already exists. skip it
                    io:format("File was already fetched. skip.~n"),
                    0;
                false ->
                    filelib:ensure_dir(Filename),
                    file:write_file(Filename, ""), % create the file
                    case my_utils:resolve_host(Host) of
                        error ->
                            io:format("DNS Resolv failed: ~p~n", [Host]),
                            0;
                        IP ->
			    fetch_manager:post_request(whereis(fetcher), IP, HttpUri, Filename),
                            1
                    end
            end;
        {error, _} ->
            io:format("Invalid URL: ~p~n", [URL]),
            0
    end.


start() ->
    crypto:start(),
    %error_logger:logfile({open, ErrorLog}),
    error_logger:tty(true),

    FetcherPid = spawn(fun() -> fetch_manager:start(settings:max_conns()) end),
    register(fetcher, FetcherPid),
    loop(0, settings:max_outstanding()).
