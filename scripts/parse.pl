use Modern::Perl;
use lib "/home/mishin/github/Datastage-DsxParse/lib";
#use FindBin;                 # locate this script
#use lib "$FindBin::Bin/..";  # use the parent directory
use Datastage::DsxParse qw(debug parse_dsx);
use Carp::Always;
use Data::TreeDumper;
#use re 'debug';
use Data::Dumper;


my $file_name = shift or die "Usage: $0 dsx_file_name\n";


my $dsx_structure = parse_dsx($file_name);
#print Dumper $dsx_structure;
#@parse_dsx=(1,2);
#debug(1,\@parse_dsx);
debug( 1, $dsx_structure );
#print DumpTree( $dsx_structure, '$dsx_structure' );
