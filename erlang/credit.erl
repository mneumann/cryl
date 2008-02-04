-module(credit).
-export([new/0, new/1, put/2, get/2, cnt/1]).

new() -> spawn(fun() -> loop(0) end).

new(Name) -> register(Name, new()).

put(Pid, N) when is_atom(Pid) -> ?MODULE:put(whereis(Pid), N);
put(Pid, N) -> Pid ! {put, N}, N.

get(Pid, N) when is_atom(Pid) -> get(whereis(Pid), N);
get(Pid, N) -> 
  Pid ! {get, N, self()},
  receive 
    {credit, N, Pid} -> N
  end.

cnt(Pid) when is_atom(Pid) -> cnt(whereis(Pid));
cnt(Pid) ->
  Pid ! {cnt, self()},
  receive
    {cnt, N, Pid} -> N
  end.

loop(Credit) ->
  receive
    {put, N} -> loop(Credit+N);
    {get, N, Pid} when Credit >= N -> Pid ! {credit, N, self()}, loop(Credit-N);
    {cnt, Pid} -> Pid ! {cnt, Credit, self()}, loop(Credit) 
  end.
