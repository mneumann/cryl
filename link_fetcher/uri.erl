-module(uri).
-export([parse/1, request_uri/1, normalize_to_s/1, to_filename/1, to_filename/2]).
-include("uri.hrl").
-define(SPLIT_AT, 3).

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
    HexDigest = my_utils:binary_to_hex(crypto:sha(NormURI)),
    D1 = string:substr(HexDigest, 1,2),
    D2 = string:substr(HexDigest, 3,2),
    D3 = string:substr(HexDigest, 5,2),
    Digest = string:substr(HexDigest, 7),
    Dom = lists:flatmap(fun (DomPart) -> split_domain_part(DomPart) end, HttpUri#http_uri.host_tokrl),
    filename:join(Dom ++ ["_", D1, D2, D3, Digest]).

to_filename(HttpUri, Root) ->
    filename:join(Root, to_filename(HttpUri)).

split_domain_part(Str, List) when (length(Str) =< ?SPLIT_AT) ->
  [Str ++ "." | List];

split_domain_part(Str, List) ->
  split_domain_part(string:substr(Str, ?SPLIT_AT+1),
                    [string:substr(Str, 1, ?SPLIT_AT) | List]).

split_domain_part(Str) ->
  lists:reverse(split_domain_part(Str, [])).
