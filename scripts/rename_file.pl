use Modern::Perl;
use Config;
use File::Slurp qw(read_dir);
if ($Config{osname} eq 'linux') {
    use lib "/home/mishin/github/Datastage-DsxParse/lib";
}
elsif ($Config{osname} eq 'MSWin32') {
    use lib
      "c:/Users/rb102870/Documents/job/bin/dsx/projects/Datastage-DsxParse-master/lib";

    use File::Slurp qw(write_file read_file);
}
use Datastage::DsxParse qw(debug split_by_header_and_job get_name_and_body);
use Data::Dumper;
# my $file_name = shift or die "Usage: $0 dsx_file_name\n";

sub get_job_name {
    my $data           = read_file(shift);
    my $header_and_job = split_by_header_and_job($data);
    my $name_and_body  = get_name_and_body($header_and_job->{job});
    return $name_and_body->{identifier};
}

# raname
# my @files = grep { /^RE .+msg$/ } readdir(DIR);
my $my_dir =
  'c:/Users/rb102870/Documents/job/bin/dsx/projects/Datastage-DsxParse-master/scripts/data/CRZ_GR1';
my @files = read_dir($my_dir);
for (grep {/^mail/} @files) {
    my $new = get_job_name($_);
    rename($_, $new . '.dsx') or print "Error renaming $_ to $new: $!\n";
}

#rename( $_, $text )  for grep(/^mail/, @files);
# my @files = read_dir($my_dir) ;
# prepend_file( $_, $text )  for grep(/^mail/, @files);
# print Dumper $name_and_body ;
# my $header_fields  = split_fields_by_new_line($header_and_job->{header});
# debug(1,$name_and_body->{identifier}); 
