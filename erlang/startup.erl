-module(startup).
-export([create/0, start/0]).
-include("defs.hrl").

%
% Create a worker directories on each node.
%
create() ->
    lists:map(fun(Node) ->
        rpc:call(Node, worker, create, [
            node_root(Node),
            13, 1000, 10000
        ])
        end, ?NODES).

%
% Start a worker on each node.
%
start() ->
    lists:map(fun(Node) ->
        rpc:call(Node, setup_node, start, 
            [filename:join(node_root(Node), "error.log") ]),
        rpc:call(Node, worker, open_and_start, [
            node_root(Node)
        ])
        end, ?NODES).

node_root(Node) ->
    filename:join(?ROOT_DIR, atom_to_list(Node)).
