use File::Slurp qw(prepend_file read_dir);
my $text='BEGIN HEADER
   CharacterSet "CP1251"
   ExportingTool "IBM InfoSphere DataStage Export"
   ToolVersion "8"
   ServerName "TEST-ETL"
   ToolInstanceID "FFF"
   MDISVersion "1.0"
   Date "2014-12-18"
   Time "12.05.57"
   ServerVersion "8.7"
END HEADER
BEGIN DSJOB'."\n";


my $my_dir= 'c:/Users/325/Documents/job/bin/dsx/projects/Datastage-DsxParse-master/scripts/data/333';
 my @files = read_dir($my_dir) ;
prepend_file( $_, $text )  for grep(/^mail/, @files);
