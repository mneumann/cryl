all: compile run

clean:
	rm -f *.beam *.dump

compile:
	erlc *.erl

run:
	erl -run main -noinput -noshell
