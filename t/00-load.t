use 5.006;
use strict;
use warnings;
use Test::More;
 
plan tests => 1;
 
BEGIN {
    use_ok( 'Datastage::DsxParse' ) || print "Bail out!\n";
}
 
diag( "Testing Datastage::DsxParse $Datastage::DsxParse::VERSION, Perl $], $^X" );
