#!/bin/bash

contrib=$(dirname $0)
(

    cd $contrib && (


	    (git clone https://github.com/ralight/sqlite3-pcre.git)
 	   	(cd sqlite3-pcre && [ -f "Makefile" ] && make)

        (git clone git@github.com:positively4th/p4thpymisc.git)
        #(cd p4thpymisc/contrib && [ -f "install.sh" ] && source ./install.sh)

    )
)



