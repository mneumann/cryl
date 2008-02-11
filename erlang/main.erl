-module(main).
-export([start/0]).
-include("uri.hrl").

-define(MAX_CONNS, 1000).
-define(MAX_OUTSTANDING, 1000).
-define(ROOT_DIR, "/tmp/download").

request_completed(_Request, Reason) ->
    %io:format("Request completed: ~p, ~p~n", [Request, Reason]).
    io:format("~p~n", [Reason]).

try_cleanup(0) -> 0;
try_cleanup(Outstanding) ->
    receive
        {complete, Req, Reason} ->
            request_completed(Req, Reason),
            try_cleanup(Outstanding-1)
    after 0 ->
        Outstanding
    end.

cleanup(0) -> ok;
cleanup(Outstanding) ->
    io:format("cleanup[~p]~n", [Outstanding]),
    receive
        {complete, Req, Reason} ->
            request_completed(Req, Reason),
            cleanup(Outstanding-1)
    end.

loop(File, N, Outstanding) when (Outstanding >= ?MAX_OUTSTANDING) ->
    cleanup(1),
    loop(File, N, Outstanding-1);

loop(File, N, Outstanding) ->
    Outstanding2 = try_cleanup(Outstanding),
    case io:get_line('') of
        Str when is_list(Str) ->
	    io:format("Line: ~p~n", [N]),
            Y = post_request(my_utils:strip(Str)),
            loop(File, N+1, Outstanding2+Y);
        eof ->
            cleanup(Outstanding2);
        _ ->
            io:format("ERROR get_line~n"),
            cleanup(Outstanding2)
    end.

resolve_host(Host) ->
    case inet:gethostbyname(Host) of
        {ok,{hostent,_,_,inet,4,[IP|_]}} ->
            IP;
        _ ->
            error
    end.
 
post_request(URL) ->
    case uri:parse(URL) of
        #http_uri{port = Port, host = Host}=HttpUri ->
            Filename = uri:to_filename(HttpUri, ?ROOT_DIR),
            case filelib:is_file(Filename) of
                true ->
                    % file already exists. skip it
                    io:format("File was already fetched. skip.~n"),
                    0;
                false ->
                    filelib:ensure_dir(Filename),
                    file:write_file(Filename, ""), % create the file
                    case resolve_host(Host) of
                        error ->
                            io:format("DNS Resolv failed: ~p~n", [Host]),
                            0;
                        IP ->
                            fetcher ! {req, self(), 
                                       {IP, Port, Host, uri:request_uri(HttpUri), Filename}},
                            1
                    end
            end;
        _ ->
            io:format("Invalid URL: ~p~n", [URL]),
            0
    end.

start() ->
    crypto:start(),
    Pid = spawn(fun() -> fetch_manager:start(?MAX_CONNS) end),
    register(fetcher, Pid), 
    loop(none, 0, 0),
    exit(ok).
