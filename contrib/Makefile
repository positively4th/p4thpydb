.PHONY : all sqlite3-pcre.o p4thpymisc

all: 
	make sqlite3-pcre.o
	make p4thpymisc

sqlite3-pcre.o :
	(git clone https://github.com/ralight/sqlite3-pcre.git || echo '') \
	&& make -C sqlite3-pcre

p4thpymisc: 
	(git clone git@github.com:positively4th/p4thpymisc.git || echo) \
	&& [ -f "p4thpymisc/Makefile" ] && make -C p4thpymisc

clean: 
	rm -rf sqlite3-pcre \
	rm -rf p4thpymisc 


