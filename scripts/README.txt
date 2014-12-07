perl -d:NYTProf -I../lib parse.pl data/example.dsx > parse.log 2>&1
nytprofhtml --open
