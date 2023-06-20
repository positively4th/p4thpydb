#!/bin/bash

(
    git clone https://github.com/ralight/sqlite3-pcre.git \
	&& cd sqlite3-pcre \
	&& make
)

# Optional for SubprocessHelper
#(
#    git clone ssh://git@p4th.net/home/git/p4thpy.git
#)
