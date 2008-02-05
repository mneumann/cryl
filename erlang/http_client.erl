%
% Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
%
% TODO:
%
%   * Send Date: header
%   * Accept gzip transfer-encoding
%

-module(http_client).
%-export([start/0]).
-export([download/2]).
-record(state, {version, code, content_length, chunked, content_type}). 
-define(CRLF, "\r\n").

%start() ->
%    download({"www.ntecs.de", 80, "www.ntecs.de", "/"}, "/tmp/www.ntecs.de").

default_state() ->
  #state{version = undefined,
         code = undefined,
         content_length = undefined,
         chunked = false}.
         
download({IP, Port, Host, ReqURI}, Filename) -> 
    case gen_tcp:connect(IP, Port, [binary, {packet, http}, {active, false}]) of
        {ok, Sock} ->
            Req = construct_request(Host, ReqURI),
                case gen_tcp:send(Sock, Req) of
                    ok ->
                        recv_head(Sock, Filename, default_state());
                    {error, Msg} ->
                        gen_tcp:close(Sock),
                        {error, {send_request, Msg}}
                end;
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
                    200 ->
                        recv_head(Sock, Filename,
                                  State#state{version=Version, code=Code});
                    _   ->
                        gen_tcp:close(Sock),
                        {fail, invalid_code}  
                end;
            {http_header, _, Key, _, Val} ->
                case header({Key, Val}, State) of
                    {ok, NewState} -> 
                        recv_head(Sock, Filename, NewState);
                    {fail, Msg}=M ->
                        gen_tcp:close(Sock),
                        M
                end;
            http_eoh ->
                recv_body(Sock, Filename,
                          {State#state.content_length,
                           State#state.chunked});
            _ ->
                gen_tcp:close(Sock), 
                {fail, invalid_header}
            end;
        {error, Msg} ->
            gen_tcp:close(Sock),
            {error, {recv_response, Msg}}
    end.

header({"Content-Type", _Type}, State) ->
    {ok, State};

header({"Transfer-Encoding", "chunked"}, State) ->
    {ok, State#state{chunked = true}};

header({"Content-Length", CLen}, State) ->
    case string:to_integer(CLen) of
      {Len, ""} when is_integer(Len) and (Len >= 0) ->  
          {ok, State#state{content_length = Len}};
      _ ->
          {fail, parse_error_content_length}
    end;

header({_,_}, State)
    -> {ok, State}.

recv_body(Sock, Filename, C) ->
    case file:open(Filename, [write, raw, compressed]) of
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
        {error, _Msg} -> % TODO: handle errors
            gen_tcp:close(Sock),
            ok
    end.

recv_data(Sock, _IO, 0) -> 
    gen_tcp:close(Sock),
    ok;

recv_data(Sock, IO, Len) when (Len > 0) ->
    case gen_tcp:recv(Sock, Len) of
        {ok, Data} ->
            file:write(IO, Data),
            recv_data(Sock, Len - size(Data));
        {error, Msg} ->
            {error, Msg}
    end;

recv_data(Sock, _IO, _) ->
    gen_tcp:close(Sock),
    {error, unexpected}.

recv_chunk(Sock, IO) ->
    inet:setopts(Sock, [{packet, line}]),
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
            case string:to_integer(binary_to_list(Line)) of
                {error, _} ->
                    gen_tcp:close(Sock),
                    {error, chunk_failure};
                {0, _} ->
                    inet:setopts(Sock, [{packet, http}]),
                    recv_trailer(Sock),
                    gen_tcp:close(Sock),
                    ok;
                {Len, _} ->
                    inet:setopts(Sock, [{packet, raw}]),
                    case recv_chunk_data(Sock, IO, Len) of
                        ok ->
                            recv_chunk(Sock, IO);
                        Msg -> 
                            gen_tcp:close(Sock),
                            Msg
                    end
            end;
        _ ->
            gen_tcp:close(Sock),
            {error, failed_to_read_chunk_header}
    end.

recv_trailer(_Sock) ->
    todo.

recv_chunk_data(_Sock, _IO, 0) ->
    ok;

recv_chunk_data(Sock, IO, Len) when (Len > 0) ->
    case gen_tcp:recv(Sock, Len) of
        {ok, Data} ->
            file:write(IO, Data),
            recv_chunk_data(Sock, IO, Len - size(Data));
        {error, Msg} ->
            {error, Msg}
    end;

recv_chunk_data(_Sock, _IO, _) -> 
    {error, invalid_chunk_len}.

