%
% Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
%
-module(my_utils).
-export([binary_to_hex/1, filename_ensure/1, 
         each_line/2, each_line/3,
         each_line_with_index/2, each_line_with_index/3,
         stdin_each_line/1, stdin_each_line/2,
         stdin_each_line_with_index/1, stdin_each_line_with_index/2,
         strip/1,
         hex_string_to_integer/1, max/2, min/2, chomp/1, resolve_host/1]).

%
% Converts a number in the range of 0-15 to it's 
% ASCII hex representative.
%
hex_digit_chr(N) when (N >= 0)  and (N < 10) -> $0 + N;      
hex_digit_chr(N) when (N >= 10) and (N < 16) -> $a - 10 + N;
hex_digit_chr(_N) -> throw(invalid_hex_digit).

%
% Convert a hex character to it's decimal value.
%
hex_chr_to_int(N) when (N >= $0) and (N =< $9) -> N - $0;
hex_chr_to_int(N) when (N >= $A) and (N =< $F) -> 10 + N - $A;
hex_chr_to_int(N) when (N >= $a) and (N =< $f) -> 10 + N - $a;
hex_chr_to_int(_N) -> error. 

%
% Example:
%   [$F, $F, $Z] -> [15, 15]
%
hexdigit_substring([]) -> [];
hexdigit_substring([C|T]) ->  
    case hex_chr_to_int(C) of
        error -> []; 
        N ->   
            [N|hexdigit_substring(T)]
    end.

sumdigits([], _Fact, Sum, _Base) -> Sum;
sumdigits([H|T], Fact, Sum, Base) ->
    sumdigits(T, Fact * Base, Sum + H * Fact, Base).  

%
% Converts a string in hex (e.g. "00FE") to
% it's decimal value.
%
hex_string_to_integer(L) ->
    sumdigits(lists:reverse(hexdigit_substring(L)), 1, 0, 16).

%
% Converts a Binary into a hexadecimal representation.
%
binary_to_hex(<<>>) -> [];
binary_to_hex(<<B, Rest/binary>>) ->
    [hex_digit_chr((B bsr 4) band 15),
     hex_digit_chr(B band 15) |
     binary_to_hex(Rest)].

%
% Removes "dangerous" characters from a filename.
% Filename MUST NOT include "/", which would otherwise
% get removed.
%
filename_ensure([]) -> [];
filename_ensure([H|T]) ->
    if ((H == $-) orelse
       ((H >= $0) andalso (H =< $9)) orelse 
       ((H >= $a) andalso (H =< $z)) orelse
       ((H >= $A) andalso (H =< $Z))) -> [H|filename_ensure(T)];
       true -> filename_ensure(T)
    end.

%
% Iterating over the lines of a file with index.
%
each_line_with_index(File, Fun) -> each_line_with_index(File, Fun, all).

each_line_with_index(File, Fun, MaxCnt) -> each_line_with_index(File, Fun, MaxCnt, 0).

each_line_with_index(File, Fun, MaxCnt, Cnt) when (MaxCnt == all) orelse (Cnt < MaxCnt) ->  
  case io:get_line(File, '') of
    eof -> Cnt;
    Str -> Fun(Str, Cnt), each_line_with_index(File, Fun, MaxCnt, Cnt+1)
  end;
each_line_with_index(_File, _Fun, _MaxCnt, Cnt) -> Cnt.

%
% Same as the version without stdin_ as prefix, but for standard input.
%
stdin_each_line_with_index(Fun) -> stdin_each_line_with_index(Fun, all).

stdin_each_line_with_index(Fun, MaxCnt) -> stdin_each_line_with_index(Fun, MaxCnt, 0).

stdin_each_line_with_index(Fun, MaxCnt, Cnt) when (MaxCnt == all) orelse (Cnt < MaxCnt) ->  
  case io:get_line('') of
    eof -> Cnt;
    Str -> Fun(Str, Cnt), stdin_each_line_with_index(Fun, MaxCnt, Cnt+1)
  end;
stdin_each_line_with_index(_Fun, _MaxCnt, Cnt) -> Cnt.

%
% Iterating over the lines of a file. 
%
each_line(File, Fun) -> each_line_with_index(File, fun(L,_) -> Fun(L) end).

each_line(File, Fun, MaxCnt) -> each_line_with_index(File, fun(L,_) -> Fun(L) end, MaxCnt).

%
% Same as the version without stdin_ as prefix, but for standard input.
%
stdin_each_line(Fun) -> stdin_each_line_with_index(fun(L,_) -> Fun(L) end).

stdin_each_line(Fun, MaxCnt) -> stdin_each_line_with_index(fun(L,_) -> Fun(L) end, MaxCnt).


%
% Removes leading and trailing whitespace (not just spaces). 
%
strip(L) -> lists:reverse(strip_leading(lists:reverse(strip_leading(L)))).

strip_leading([])     -> [];
strip_leading([32|T]) -> strip_leading(T);
strip_leading([10|T]) -> strip_leading(T);
strip_leading([13|T]) -> strip_leading(T);
strip_leading([9|T])  -> strip_leading(T);
strip_leading([11|T]) -> strip_leading(T);
strip_leading(L)      -> L.

max(A,  B) when (A >= B) -> A; 
max(_A, B) -> B.

min(A,  B) when (A =< B) -> A; 
min(_A, B) -> B.

%
% Removes the last character if and only if it is a newline
% character
%
chomp([]) -> [];
chomp([H]=L) ->
    case H of
        13 -> [];
        10 -> [];
        _  -> L
    end;
chomp([H|T]) ->
    [H|chomp(T)].

%
% Resolves a domain name via DNS into an IPv4 address
% or returns error.
%
resolve_host(Host) ->
    case inet:gethostbyname(Host) of
        {ok,{hostent,_,_,inet,4,[IP|_]}} ->
            IP;
        _ ->
            error
    end.
