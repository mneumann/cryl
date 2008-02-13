-module(main).
-export([start/0]).

start() ->
    crypto:start(),
    Worker = worker:open_and_start("./work"),
    worker:submit("http://www.google.de/", Worker).
