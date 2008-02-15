#!/bin/sh

# expects an existing $HOME/.erlang.cookie file

erl -name `whoami`@`hostname`.`hostname -d` 
