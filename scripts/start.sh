perl parse.pl data/example.dsx > example.log
perl t/01_compare_log.t example.log.orig example.log
