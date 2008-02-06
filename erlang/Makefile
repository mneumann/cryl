all: compile run

clean:
	rm *.beam *.dump

compile:
	erlc *.erl

run:
	erl -run main -noinput -noshell
