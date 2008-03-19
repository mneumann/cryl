-module(settings).

-define(DEFAULT_MAX_CONNS, 1000).
-define(DEFAULT_MAX_OUTSTANDING, 1000).

-export([max_conns/0, max_outstanding/0]).

parse_int(EnvKey, Default) ->
    case os:getenv(EnvKey) of
    false ->
        Default;
    Str ->
        case string:to_integer(Str) of
            {Num, []} ->
                Num;
            _ ->
                error_logger:error_msg("Invalid ~s env~n", [EnvKey]),
                Default
        end
    end.

max_conns() ->
    parse_int("CRYL_MAX_CONNS", ?DEFAULT_MAX_CONNS).

max_outstanding() ->
    parse_int("CRYL_MAX_OUTSTANDING", ?DEFAULT_MAX_OUTSTANDING).
