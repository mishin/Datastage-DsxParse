package Datastage::DsxParse;
use 5.010;
use strict;
use warnings;
use Data::TreeDumper;
use File::Slurp qw(write_file read_file);

our $VERSION = "0.01";
use Sub::Exporter -setup => {
    exports => [
        qw/
          debug
          parse_dsx
          /
    ],
};

sub parse_dsx {
    my ($file_name)    = @_;
    my $data           = read_file($file_name);
    my $header_and_job = split_by_header_and_job($data);
    my $header_fields  = split_fields_by_new_line( $header_and_job->{header} );
    my $name_and_body  = get_name_and_body( $header_and_job->{job} );
    my $ref_array_dsrecords = parse_records( $name_and_body->{job_body} );
    my $rich_records        = enrich_records($ref_array_dsrecords);

  #    my @records             = ();
  #    for my $rec ( @{$ref_array_dsrecords} ) {
  #        my %rich_fields = ();
  #        my $big_field   = split_fields_by_new_line($rec);
  #        $rich_fields{ $big_field->{Name} . '_' . $big_field->{Identifier} } =
  #          $big_field;
  #        push @records, \%rich_fields;
  #    }

    return $rich_records;
}

sub enrich_records {
    my $ref_array_dsrecords = shift;
    my %richer_record       = ();
    for my $rec ( @{$ref_array_dsrecords} ) {
        my ( $identifier, $fields ) = get_identifier_and_field_of_record($rec);
        $richer_record{$identifier} = $fields;
    }
    return \%richer_record;
}

sub get_identifier_and_field_of_record {
    my $data = shift;
    $data =~ /
(?<record_fields_body1>
BEGIN[ ]DSRECORD\n\s+
Identifier[ ]"(?<identifier>\w+)"
.*?
)
(?<dsrecord_body>
BEGIN[ ]DSSUBRECORD
.*?
END[ ]DSSUBRECORD[\n]
)
(?<record_fields_bodys2>
.*?
END[ ]DSRECORD
)
/xsg;
    my %fields = %+;

    return ( $fields{identifier}, \%fields );

}

sub split_by_subrecords {
    my $curr_record = shift;
    local $/ = '';    # Paragraph mode
    my @dssubrecords = ( $curr_record =~
          / ( BEGIN[ ]DSSUBRECORD[\n]   .*?  END[ ]DSSUBRECORD ) /xsg );
    return \@dssubrecords;
}

sub get_name_and_body {
    my $data = shift;
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
    my %name_and_body = %+;

    return \%name_and_body;
}

sub split_fields_by_new_line {
    my ($curr_record) = @_;

#удаляем ненужные begin end
#    $curr_record =~ s/BEGIN[ ]DSSUBRECORD[\n]  (.*?) END[ ]DSSUBRECORD /$1/xsg;
    my @records = split( /\n/, $curr_record );
    my %big_hash = ();
    for my $line (@records) {
        while ( $line =~ m/(?<name>\w+)[ ]"(?<value>.*?)(?<!\\)"/xsg ) {
            my $value = '';
            if ( defined $+{value} ) {
                $value = clear_from_back_slash( $+{value} );
            }
            $big_hash{ $+{name} } = $value;
        }
    }
    return \%big_hash;
}

sub clear_from_back_slash {
    my $string = shift;
    $string =~ s#\\(['"])#$1#g;
    return $string;
}

sub split_by_header_and_job {
    my $data = shift;
    local $/ = '';    # Paragraph mode
    my %header_and_job = ();
    my @fields         = ();

    #@fields = (
    $data =~ / 
(?<header>
BEGIN[ ]HEADER
.*?
END[ ]HEADER
)
.*?
(?<job> 
BEGIN[ ]DSJOB   
.*?  
END[ ]DSJOB )

 /xsg
      ;

    #    @header_and_job{ 'header', 'job' } = @fields[ 0 .. 1 ];
    %header_and_job = %+;

    #return \@fields;
    return \%header_and_job;
}

sub parse_records {
    my $data = shift;
    local $/ = '';    # Paragraph mode
    my @records =
      ( $data =~ / ( BEGIN[ ]DSRECORD[\n]   .*?  END[ ]DSRECORD ) /xsg );
    return \@records;
}

sub debug {
    my ( $run_as_a_one, $value ) = @_;
    state $i= 1;
    if ( ( $i == 1 ) || ( $run_as_a_one != 1 ) ) {
        dump_in_html($value);
    }
    $i++;
}

sub dump_in_html {
    my ($job_and_formats) = @_;

#-------------------------------------------------------------------------------

# the renderer can return a default style. This is needed as styles must be at the top of the document
    my $style = '';
    my $body  = DumpTree(
        $job_and_formats, 'Data'
        ,
        DISPLAY_ADDRESS      => 0,
        DISPLAY_ROOT_ADDRESS => 1

          #~ , DISPLAY_PERL_ADDRESS => 1
          #~ , DISPLAY_PERL_SIZE => 1
        ,
        RENDERER => {
            NAME   => 'DHTML',
            STYLE  => \$style,
            BUTTON => {
                COLLAPSE_EXPAND => 1,
                SEARCH          => 1
            }
        }
    );

    my $body2  = '';
    my $style2 = '';
    $body2 = DumpTree(
        $job_and_formats, 'Data2'
        , DISPLAY_ROOT_ADDRESS => 1

          #~ , DISPLAY_PERL_ADDRESS => 1
          #~ , DISPLAY_PERL_SIZE => 1
        ,
        RENDERER => {
            NAME      => 'DHTML',
            STYLE     => \$style2,
            COLLAPSED => 1,
            CLASS     => 'collapse_test',
            BUTTON    => {
                COLLAPSE_EXPAND => 1,
                SEARCH          => 1
            }
        }
    );

    my $dump = <<"EOT";
<?xml version="1.0" encoding="iso-8859-1"?>
<!DOCTYPE html 
     PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
     "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"
>
     
<html>
<!-- Automatically generated by Perl and Data::TreeDumper::Renderer::DHTML-->
<head>
<title>Data</title>
$style
$style2
</head>
<body>

$body
$body2

</body>
</html>
EOT

    write_file_utf8( 'dump.html', $dump );

#-------------------------------------------------------------------------------

}

sub write_file_utf8 {

    my $name   = shift;
    my $string = shift;
    my $ustr   = $string;    #"simple unicode string \x{0434} indeed";

    {
        open( my $FH, ">:encoding(UTF-8)", $name )
          or die "Failed to open file - $!";

        write_file( $FH, $ustr )
          or warn "Failed write_file";
    }
}

1;
__END__

=pod

=head1 NAME

Datastage::DsxParse - abstract

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SEE ALSO

=head1 REPOSITORY

L<https://github.com/mishin/Datastage-DsxParse>

=head1 BUGS AND FEATURE REQUESTS

Please report bugs and feature requests to my Github issues
L<https://github.com/mishin/Datastage-DsxParse/issues>.

Although I prefer Github, non-Github users can use CPAN RT
L<https://rt.cpan.org/Public/Dist/Display.html?Name=Datastage-DsxParse>.
Please send email to C<bug-Datastage-DsxParse at rt.cpan.org> to report bugs
if you do not have CPAN RT account.


=head1 AUTHOR
 
Nikolay Mishin, C<< <mi at ya.ru> >>


=head1 LICENSE AND COPYRIGHT

Copyright 2014 Nikolay Mishin.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.


=cut

