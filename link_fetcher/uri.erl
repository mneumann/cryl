-module(uri).
-export([parse/1, request_uri/1, normalize_to_s/1, to_filename/1, to_filename/2]).
-include("uri.hrl").

allowed_host_characters([]) -> [];
allowed_host_characters([H|T]) ->
    if ((H == $-) orelse
       ((H >= $0) andalso (H =< $9)) orelse 
       ((H >= $a) andalso (H =< $z)) orelse
       ((H >= $A) andalso (H =< $Z))) -> [H|allowed_host_characters(T)];
       true -> allowed_host_characters(T)
    end.

parse(URI) ->
    case http_uri:parse(URI) of
        {http, [], Host, Port, Path, Query} ->
            HostLower = string:strip(string:to_lower(Host)),
            Host2 = string:tokens(HostLower, "."),
            Host3 = lists:reverse(lists:map(fun allowed_host_characters/1, Host2)),
            #http_uri{
                orig = URI,
                host = Host,
                host_lower = HostLower,
                host_tokrl = Host3,
                port = Port,
                path = Path,
                queryy = Query};
        _ -> 
            {error, invalid_url}
    end.

request_uri(HttpUri) ->
    HttpUri#http_uri.path ++ 
    HttpUri#http_uri.queryy.

normalize_to_s(HttpUri) ->
    "http://" ++ HttpUri#http_uri.host_lower ++ 
    ":" ++ integer_to_list(HttpUri#http_uri.port) ++
    request_uri(HttpUri).
   
%
% crypto:sha requires crypto:start().
%
to_filename(HttpUri) ->
    NormURI = normalize_to_s(HttpUri),
    [D1, D2 | Digest] = my_utils:binary_to_hex(crypto:sha(NormURI)),
    filename:join([filename:join(HttpUri#http_uri.host_tokrl), [D1, D2], Digest]).

to_filename(HttpUri, Root) ->
    filename:join(Root, to_filename(HttpUri)).
