-module(fetcher).
-export([fetch/2]).
-define(TIMEOUT, 10000).

fetch(URL, Basename) ->
  R = http:request(get, {URL, []}, [{timeout, 5000}], [{stream, {self, once}}, {sync, false}]),
  case R of
    {ok, Ref} ->
      %link(Ref),

      case file:open(Basename, [write, raw, compressed]) of 
        {ok, IO} -> 
          loop(Ref, IO),
          file:close(IO);
        Err ->
          io:format("ERROR in file:open: ~p~n", [Err])
        end;
     
    Err ->
      io:format("ERROR in http:request: ~p~n", [Err])
  end.

loop(Ref, IO) ->
  receive
    {http, R} -> 
      case handle(R, IO) of
        ok -> loop(Ref, IO);
        X  -> X
      end;
    A ->
        %io:format("INV: ~p~n", [A]),
        error
  after 20000 ->
    io:format("TIMEOUT: ~n"),
    timeout
  end.

handle({Ref, stream, Data},          IO) -> file:write(IO, Data);
handle({Ref, stream_start, Header, Pid}, _IO) -> link(Pid), ok;
handle({Ref, stream_end, Header},   _IO) -> success;
handle({Ref, Failure},              _IO) -> error;
handle(_, _IO) -> error.
