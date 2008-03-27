parse.o: parse.c parse.h
	${CC} -c parse.c

link_extractor: link_extractor.c parse.c parse.h
	${CC} -o link_extractor link_extractor.c parse.c

example.links: link_extractor example.html
	echo example.html | ./link_extractor 

clean:
	rm -f link_extractor example.links *.o
