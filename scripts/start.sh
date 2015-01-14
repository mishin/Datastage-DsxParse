perl parse.pl $1 > $1.log 2>$1.err.log
perl t/01_compare_log.t $1.log.orig $1.log
