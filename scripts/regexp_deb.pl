use strict;
use warnings;
use 5.010;
use re 'debug';

# using the same strings as the question's image for reference:

my $str = 'Even if I do say so myself: "RegexBuddy is awesome"';
$str =~ /(Regexp?Buddy is (awful|acceptable|awesome))/;
