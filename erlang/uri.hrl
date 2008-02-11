%
% host_tokrl ::= host_tokens reverse lowercase
%
-record(http_uri, {orig, host, host_lower, host_tokrl, port, path, queryy}).
