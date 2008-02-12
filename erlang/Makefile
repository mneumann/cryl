all: compile run

clean:
	rm -f *.beam *.dump
	rm -rf /tmp/download ./work

compile:
	erlc *.erl

run:
	erl -run main -noinput -noshell < /home/mneumann/adbrite-urls.txt
