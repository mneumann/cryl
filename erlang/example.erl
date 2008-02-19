-module(example).
-export([start/0]).

start() ->
  Nodes = [node()],
  startup:create(Nodes),
  startup:start(Nodes),
  submitter:start(Nodes, "/home/mneumann/adbrite-urls.txt").
