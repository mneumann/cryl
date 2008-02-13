all: compile run

clean:
	rm -rf *.beam *.dump /tmp/cryl

compile:
	erlc *.erl

run:
	erl -run main -noinput -noshell < /home/mneumann/adbrite-urls.txt
