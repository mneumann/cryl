-module(main).
-export([start/0]).

-define(MAX_CONNS, 1000).
-define(MAX_OUTSTANDING, 10000).
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
        eof -> 
            cleanup(Outstanding2);
        Str ->
	    io:format("Line: ~p~n", [N]),
            Y = post_request(my_utils:strip(Str)),
            loop(File, N+1, Outstanding2+Y)
    end.

resolve_host(Host) ->
    case inet:gethostbyname(Host) of
        {ok,{hostent,_,_,inet,4,[IP|_]}} ->
            IP;
        _ ->
            error
    end.
 
post_request(URL) ->
    case url_to_path(URL) of
        {ok, Basename, _URL2, {Host,Port,ReqURI}} -> 
            Filename = filename:join(?ROOT_DIR, Basename),
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
                                       {IP, Port, Host, ReqURI, Filename}},
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

digest(Bin) ->
  my_utils:binary_to_hex(crypto:sha(Bin)).

url_to_path(URL) ->
    case http_uri:parse(URL) of 
      {http, [], Host, Port, Path, Query} -> 
          HostLow = string:to_lower(Host),
          HostToks = lists:map(fun string:strip/1, string:tokens(HostLow, ".")),
          HostLow2 = string:join(HostToks, "."),
          HostToks2 = lists:reverse(lists:map(fun my_utils:filename_ensure/1, HostToks)),
          URL2 = "http://" ++ HostLow2 ++ ":" ++ integer_to_list(Port) ++ Path ++ Query, % normalize URL
          URLHash = digest(URL2),
          {ok, filename:join(HostToks2 ++ [URLHash]), URL2, {Host, Port, Path ++ Query}};
      _ -> {error, invalid_url}
    end.
