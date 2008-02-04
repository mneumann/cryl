%
% Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
%
-module(my_utils).
-export([binary_to_hex/1, filename_ensure/1, each_line/2, each_line/3,
         each_line_with_index/2, each_line_with_index/3, strip/1]).

%
% Converts a number in the range of 0-15 to it's 
% ASCII hex representative.
%
hex_digit_chr(N) when (N >= 0)  and (N < 10) -> $0 + N;      
hex_digit_chr(N) when (N >= 10) and (N < 16) -> $a - 10 + N;
hex_digit_chr(_N) -> throw(invalid_hex_digit).

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
% Iterating over the lines of a file. 
%
each_line(File, Fun) -> each_line_with_index(File, fun(L,_) -> Fun(L) end).

each_line(File, Fun, MaxCnt) -> each_line_with_index(File, fun(L,_) -> Fun(L) end, MaxCnt).


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
