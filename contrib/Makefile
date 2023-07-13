.PHONY : all sqlite3-pcre.o p4thpymisc

all: sqlite3-pcre.o p4thpymisc

sqlite3-pcre.o :
	    (git clone https://github.com/ralight/sqlite3-pcre.git || echo '') \
		&& (cd sqlite3-pcre && git pull) \
		&& make -C sqlite3-pcre

p4thpymisc : 
        (git clone https://github.com/positively4th/p4thpymisc.git || echo) \
		&& (cd p4thpymisc && git pull) \
		&& [ -f "Makefile" ] && make -C p4thpymisc

clean: 
	rm -rf sqlite3-pcre \
	rm -rf p4thpymisc 


