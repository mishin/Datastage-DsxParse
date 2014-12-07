use strict;
use warnings;
use 5.010;
use re 'debug';

# using the same strings as the question's image for reference:
 my ($file_name)         =shift  or die "Usage: $0 dsx_file_name\n";
use File::Slurp qw(write_file read_file);
    my $data                = read_file($file_name);

 local $/ = '';    # Paragraph mode

#my $str = 'Even if I do say so myself: "RegexBuddy is awesome"';
 $data =~ /
BEGIN[ ]DSJOB\n\s+
Identifier[ ]"(?<identifier>\w+)"
.*?
(?<job_body>
BEGIN[ ]DSRECORD
.*?
END[ ]DSRECORD[\n]
)
END[ ]DSJOB
/xsg;

#$str =~ /(Regexp?Buddy is (awful|acceptable|awesome))/;
