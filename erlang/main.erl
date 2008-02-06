-module(main).
-export([start/0]).

-define(MAX_FETCHES, 100).
-define(ROOT_DIR, "/tmp/download").
-define(URL_FILE, "/home/mneumann/adbrite-urls.txt").

request_completed(Request, Reason) ->
    %io:format("Request completed: ~p, ~p~n", [Request, Reason]).
    io:format("~p~n", [Reason]).

cleanup(0) -> ok;
cleanup(Outstanding) ->
    receive
        {complete, Req, Reason} ->
            request_completed(Req, Reason),
            cleanup(Outstanding-1)
    end.

loop(File, N, Outstanding) when (Outstanding >= ?MAX_FETCHES) ->
    receive
        {complete, Req, Reason} ->
            request_completed(Req, Reason),
            loop(File, N, Outstanding-1)
    end;

loop(File, N, Outstanding) ->
    %receive
    %    {complete, Req, Reason} ->
    %        request_completed(Req, Reason),
    %        loop(File, N, Outstanding-1)
    %after 0 -> 
    %    skip
    %end,

    case io:get_line(File, '') of
        eof -> 
            cleanup(Outstanding);
        Str ->
            Y = post_request(my_utils:strip(Str)),
            loop(File, N, Outstanding+Y)
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
    Pid = spawn(fun() -> fetch_manager:start(?MAX_FETCHES) end),
    register(fetcher, Pid), 
    {ok, File} = file:open(?URL_FILE, read),
    loop(File, 0, 0).

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
