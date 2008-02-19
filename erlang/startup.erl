-module(startup).
-export([create/1, start/1]).
-include("defs.hrl").

%
% Create a worker directories on each node.
%
create(Nodes) ->
    lists:map(fun(Node) ->
        rpc:call(Node, worker, create, [
            node_root(Node),
            27, 1000, 10000
        ])
        end, Nodes).

%
% Start a worker on each node.
%
start(Nodes) ->
    lists:map(fun(Node) ->
        rpc:call(Node, setup_node, start, 
            [filename:join(node_root(Node), "error.log") ]),
        rpc:call(Node, worker, open_and_start, [
            node_root(Node)
        ])
        end, Nodes).

node_root(Node) ->
    filename:join(?ROOT_DIR, atom_to_list(Node)).
