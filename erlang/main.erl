-module(main).
-export([start/0]).
-define(MAX_FETCHES, 100).
-define(LINES, all).
-define(ROOT_DIR, "/tmp/fuckyou").
-define(URL_FILE, "/tmp/all").

%
% The dispatch process catches exists from it's fetch processes.
%
dispatch() -> 
  process_flag(trap_exit, true),
  dispatch_loop().

dispatch_loop() ->
  receive  
    {req, URL} ->
        case url_to_path(URL) of
          {ok, Basename, URL2} -> 
              Path = filename:join(?ROOT_DIR, Basename),
              filelib:ensure_dir(Path),
              %io:format("~p~n", [URL2]), 
              spawn_link(fun() -> fetcher:fetch(URL2, Path) end),
              dispatch_loop();
          {error, _} ->
              io:fwrite("Invalid URL: ~p~n", URL),
              credit:put(credit, 1),
              io:format("Credits: ~p~n", [credit:cnt(credit)]),
              dispatch_loop()
        end;

    {'EXIT', _, _} ->
        credit:put(credit, 1),
        io:format("Credits: ~p~n", [credit:cnt(credit)]),
        dispatch_loop();

    {term} -> ok;

    _ -> 
      io:format("WTF-----------------------~n")
  end.

post(Line, Pid) ->
  credit:get(credit, 1),
  Pid ! {req, Line}. 

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
          {ok, filename:join(HostToks2 ++ [URLHash]), URL2};
      _ -> {error, invalid_url}
    end.

start() ->
  crypto:start(),
  inets:start(),
  http:set_options([{max_sessions, 1}, {max_pipeline_length, 1}]),
  %process_flag(trap_exit, true),
  credit:new(credit),
  %io:format("Credit: ~p~n", [credit:cnt(credit)]),
  credit:put(credit, ?MAX_FETCHES),
  {ok, F} = file:open(?URL_FILE, read),
  Pid = spawn(fun dispatch/0),
  my_utils:each_line_with_index(F, 
    fun(Line, I) -> 
      %io:format("LINE: ~p~n", [I]),
      post(my_utils:strip(Line), Pid)
    end, ?LINES),
  io:format("Credits: ~p~n", [credit:cnt(credit)]),
  credit:get(credit, ?MAX_FETCHES),
  Pid ! {term}, 
  io:format("READY~n"),
  exit(0).
