use Modern::Perl;
use Config;
if ($Config{osname} eq 'linux') {
    use lib "/home/mishin/github/Datastage-DsxParse/lib";
}
elsif ($Config{osname} eq 'MSWin32') {
    use lib
      "c:/Users/rb102870/Documents/job/bin/dsx/projects/Datastage-DsxParse-master/lib";
}
# say $Config{osname};
use Datastage::DsxParse qw(debug parse_dsx);
use Carp::Always;
use Data::TreeDumper;

my $file_name = shift or die "Usage: $0 dsx_file_name\n";


my $dsx_structure = parse_dsx($file_name);
# debug(1, $dsx_structure);
