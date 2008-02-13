-module(setup_node).
-export([start/1]).

start(ErrorLog) ->
    crypto:start(),
    error_logger:logfile({open, ErrorLog}),
    error_logger:tty(false).
