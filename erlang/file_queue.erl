%
% File Queue implements a queuing mechanism that
% is based on multiple queue files and a "current"
% and "next" generation.
%
% An enqueue operation takes only place in the 
% "next" generation, a dequeue in the "current".
% Once the "current" queue is empty, it's swapped
% with the "next".
%
% Each "next" and "current" consists of a number
% files. Once a file in the "current" queue reaches
% the end, it is removed completely from the "current"
% queue.  
%
% The advantages are two fold:
%
%   1) Provides more fine-grained fall-back in case of
%      a shutdown.
%
%   2) Provides randomization of execution order, by simply 
%      enqueuing into a random file.
%
% Each line in a queue file is an entry.
% 
% Copyright (c) 2008 by Michael Neumann
%
-module(file_queue).
-export([setup/2]).

setup(QueueRoot, NumQueues) ->
    filelib:ensure_dir(QueueRoot),
    file:make_dir(QueueRoot),
    file:make_dir(filename:join(QueueRoot, "current")),
    file:make_dir(filename:join(QueueRoot, "next")),
    file:write_file(filename:join(QueueRoot, "iter"), "1"),
    file:write_file(filename:join(QueueRoot, "numq"), integer_to_list(NumQueues)).  
