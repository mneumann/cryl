all:
	cd link_fetcher && make
	cd link_extractor && ruby extconf.rb && make
clean:
	cd erlang && make clean
	cd link_extractor && make clean
	cd link_fetcher && make clean
