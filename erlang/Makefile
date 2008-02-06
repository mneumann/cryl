all: compile run

clean:
	rm *.beam *.dump

compile:
	erlc *.erl

run:
	rm -rf /tmp/download
	erl -run main -noinput -noshell
