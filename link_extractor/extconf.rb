require 'mkmf'
($CFLAGS ||= "") << " -I../"
($LIBS ||= "") << " ../parse.o"
create_makefile('link_parser')
