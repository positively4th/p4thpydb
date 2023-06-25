#!/bin/bash

(
    git clone https://github.com/ralight/sqlite3-pcre.git \
	&& cd sqlite3-pcre \
	&& make
)

(
	git clone https://github.com/positively4th/p4thpymisc.git
)
