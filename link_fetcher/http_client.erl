%
% Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
%
% TODO:
%
%   * Send Date: header
%   * Accept gzip transfer-encoding
%

-module(http_client).
-export([download/2]).
-record(state, {version, code, content_length, chunked, content_type,
                location}). 
-define(CRLF, <<"\r\n">>).
-define(BLOCK_SIZE, 16384).
-define(TIMEOUT, 10000). % 10 seconds

default_state() ->
  #state{version = undefined,
         code = undefined,
         content_length = undefined,
         chunked = false}.
         
download({IP, Port, Host, ReqURI}, Filename) -> 
    case gen_tcp:connect(IP, Port, 
                         [binary, {packet, http}, {active, false}],
                         ?TIMEOUT) of
        {ok, Sock} ->
            Req = construct_request(Host, ReqURI),
                Return =
                case gen_tcp:send(Sock, Req) of
                    ok ->
                        recv_head(Sock, Filename, default_state());
                    {error, Msg} ->
                        {error, {send_request, Msg}}
                end,
                gen_tcp:close(Sock),
                Return;
        {error, Msg} ->
            {error, {connect, Msg}}
  end.

%
% Construct HTTP Request
%
construct_request(Host, ReqURI) ->
    [<<"GET ">>, ReqURI, <<" HTTP/1.1">>, ?CRLF,
     <<"Host: ">>, Host, ?CRLF,
     <<"Connection: close">>, ?CRLF,
     ?CRLF].

%
% Receive Head of response.
%
recv_head(Sock, Filename, State) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Msg} -> case Msg of
            {http_response, Version, Code, _Status} -> 
                case Code of
                    N when (N == 200) or (N == 302) or (N == 301) ->
                        recv_head(Sock, Filename,
                                  State#state{version=Version, code=Code});
                    404 -> 
                        not_found;
                    _   ->
                        {fail, {invalid_code, Code}}
                end;
            {http_header, _, Key, _, Val} ->
                case header({Key, Val}, State) of
                    {ok, NewState} -> 
                        recv_head(Sock, Filename, NewState);
                    {fail, _}=M ->
                        M
                end;
            http_eoh ->
                case State#state.code of
                    200 ->
                        recv_body(Sock, Filename,
                                  {State#state.content_length,
                                   State#state.chunked});
                    301 ->
                        {redirect, State#state.location};
                    302 ->
                        {redirect, State#state.location} 
                end;
            _ ->
                {fail, invalid_header}
            end;
        {error, Msg} ->
            {error, {recv_response, Msg}}
    end.

header({'Content-Type', "text/" ++ _T}, State) ->
    {ok, State};

header({'Content-Type', Type}, _State) ->
    {fail, {invalid_content_type, Type}};

header({'Transfer-Encoding', "chunked"}, State) ->
    {ok, State#state{chunked = true}};

header({'Content-Length', CLen}, State) ->
    case string:to_integer(CLen) of
      {Len, ""} when is_integer(Len) and (Len >= 0) ->  
          {ok, State#state{content_length = Len}};
      _ ->
          {fail, parse_error_content_length}
    end;

header({'Location', Location}, State) ->
    {ok, State#state{location = Location}};

header({_K,_V}, State) ->
    %io:format("~p: ~p~n", [K, V]),
    {ok, State}.

recv_body(Sock, Filename, C) ->
    case file:open(Filename, [write, raw]) of %, compressed]) of
        {ok, IO} ->
            Res = recv_body2(Sock, IO, C),
            file:close(IO),
            Res;
        _ ->
            {error, failed_to_open_file}
    end.

%
% No Content-Length given and no Chunked encoding.
% -> Read until socket is closed.
%
recv_body2(Sock, IO, {undefined, false}) ->
    inet:setopts(Sock, [{packet, raw}]),
    recv_data(Sock, IO);

%
% Chunked encoding.
%
recv_body2(Sock, IO, {_, true}) ->
    recv_chunk(Sock, IO);

%
% Content-Length specified.
%
recv_body2(Sock, IO, {N, _}) ->
    inet:setopts(Sock, [{packet, raw}]),
    recv_data(Sock, IO, N).

%
% Read until socket is closed.
% For now just stop when en error occurs (ignore).
%
recv_data(Sock, IO) ->
     case gen_tcp:recv(Sock, 0) of
        {ok, Data} ->
            file:write(IO, Data),
            recv_data(Sock, IO);
        {error, closed} ->
            ok;
        {error, Msg} ->
            {error, {recv_data_2, Msg}}
    end.

recv_data(_Sock, _IO, 0) -> 
    ok;

recv_data(Sock, IO, Len) when (Len > 0) ->
    case gen_tcp:recv(Sock, my_utils:min(?BLOCK_SIZE, Len)) of
        {ok, Data} ->
            file:write(IO, Data),
            recv_data(Sock, Len - size(Data));
        {error, Msg} ->
            {error, {recv_data_3, Msg}}
    end;

recv_data(_Sock, _IO, _) ->
    {error, unexpected}.

recv_chunk(Sock, IO) ->
    inet:setopts(Sock, [{packet, line}]),
    case gen_tcp:recv(Sock, 0) of
        {ok, ?CRLF} ->
            % ignore empty lines
            recv_chunk(Sock, IO);
        {ok, Line} ->
            case my_utils:hex_string_to_integer(binary_to_list(Line)) of
                0 ->
                    inet:setopts(Sock, [{packet, http}]),
                    recv_trailer(Sock),
                    ok;
                Len ->
                    inet:setopts(Sock, [{packet, raw}]),
                    case recv_chunk_data(Sock, IO, Len) of
                        ok ->
                            recv_chunk(Sock, IO);
                        Msg -> 
                            Msg
                    end
            end;
        _ ->
            {error, failed_to_read_chunk_header}
    end.

recv_trailer(_Sock) ->
    todo.

recv_chunk_data(_Sock, _IO, 0) ->
    ok;

recv_chunk_data(Sock, IO, Len) when (Len > 0) ->
    case gen_tcp:recv(Sock, my_utils:min(?BLOCK_SIZE, Len)) of
        {ok, Data} ->
            file:write(IO, Data),
            recv_chunk_data(Sock, IO, Len - size(Data));
        {error, Msg} ->
            {error, {recv_chunk_data, Msg}}
    end;

recv_chunk_data(_Sock, _IO, _) -> 
    {error, invalid_chunk_len}.

