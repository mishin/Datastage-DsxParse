package Datastage::DsxParse;
use 5.010;
use strict;
use warnings;
use Data::TreeDumper;
use File::Slurp qw(write_file read_file);

#use re 'debug';

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
    my @richer_record       = ();
    for my $rec ( @{$ref_array_dsrecords} ) {
        my $fields = get_identifier_and_field_of_record($rec);
        push @richer_record, pack_fields($fields);
    }
    return \@richer_record;
}

sub pack_fields {
    my $fields      = shift;
    my %new_fields  = ();
    my $identtifier = '';
    if ( defined $fields->{identifier} ) {
        $new_fields{identifier} = $fields->{identifier};
        $new_fields{fields} =
          split_fields_by_new_line(
            $fields->{record_fields_body1} . $fields->{record_fields_body2} );
        $new_fields{subrecord_body} =
          reformat_subrecord( $fields->{subrecord_body} );
    }
    elsif ( defined $fields->{identifier2} ) {
        $new_fields{identifier} = $fields->{identifier2};
        $new_fields{fields} =
          split_fields_by_new_line( $fields->{record_fields_body} );
    }
    return \%new_fields;
}


sub get_identifier_and_field_of_record {
    my $data   = shift;
    my %fields = ();
    if (
        $data =~ /
(:?BEGIN[ ]DSRECORD\n
(?<record_fields_body1>
.*?
Identifier[ ]"(?<identifier>\w+)"
.*?)
(?<subrecord_body>
BEGIN[ ]DSSUBRECORD.*  END[ ]DSSUBRECORD)
     (?<record_fields_body2>.*?)
END[ ]DSRECORD)
|
(:?BEGIN[ ]DSRECORD\n
(?<record_fields_body>
.*?
Identifier[ ]"(?<identifier2>\w+)"
.*?)
END[ ]DSRECORD)
   /xsg
      )
    {
        %fields = %+;
    }
    return ( \%fields );

}

sub reformat_subrecord {
    my $curr_record      = shift;
    my $ref_dssubrecords = split_by_subrecords($curr_record);
    my @subrecords       = ();
    for my $subrec ( @{$ref_dssubrecords} ) {
        push @subrecords, split_fields_by_new_line($subrec);
    }
    return \@subrecords;
}

sub split_by_subrecords {
    my $curr_record = shift;
    local $/ = '';    # Paragraph mode
    my @dssubrecords = ( $curr_record =~
          / BEGIN[ ]DSSUBRECORD([\n]   .*?  )END[ ]DSSUBRECORD /xsg );
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
    my @big_array = ();
    
    while (
        $curr_record =~ m/
(?<name>\w+)[ ]"(?<value>.*?)(?<!\\)"|
((?<name2>\w+)[ ]\Q=+=+=+=\E
(?<value2>.*?)
\Q=+=+=+=\E)
        /xsg
      )
    {
        my %big_hash = ();
        my ( $value, $name ) = ( '', '' );
        if ( defined $+{name} ) {
            $name  = $+{name};
            $value = $+{value};
        }
        elsif ( defined $+{name2} ) {
            $name  = $+{name2};
            $value = $+{value2};
        }
        # $big_hash{$name} = clear_from_back_slash($value);
        $big_hash{$name} = $value;
        push @big_array,\%big_hash;
    }
    return \@big_array;
}

sub clear_from_back_slash {
    my $string = shift;
    if ( defined $string ) {
        $string =~ s#\\(['"])#$1#g;
    }
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

    %header_and_job = %+;
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

