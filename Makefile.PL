use 5.006;
use strict;
use warnings FATAL => 'all';
use inc::Module::Install;

name     'Datastage-DsxParse';
all_from 'lib/Datastage/DsxParse.pm';
author   q{Nikolay Mishin <mi@ya.ru>};
license  'perl';

perl_version 5.006;

tests_recursive('t');

resources (
   homepage   => 'http://mishin.narod.ru',
   #IRC        => 'irc://irc.perl.org/#Datastage-DsxParse',
   license    => 'http://dev.perl.org/licenses/',
   repository => 'https://github.com/mishin/Datastage-DsxParse.git
',
   bugtracker => 'http://rt.cpan.org/NoAuth/Bugs.html?Dist=Datastage-DsxParse',
);

configure_requires (
   'Module::Install' => 0,
);

build_requires (
   'Test::More' => 0,
);

requires (
   #'ABC'              => 1.6,
   #'Foo::Bar::Module' => 5.0401,
);

install_as_cpan;
auto_install;
WriteAll;
