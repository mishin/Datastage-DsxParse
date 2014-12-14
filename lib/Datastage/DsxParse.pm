package Datastage::DsxParse;
use 5.010;
use utf8;
use strict;
use warnings;
use Data::TreeDumper;
use Data::Dumper;
use File::Slurp qw(write_file read_file);
use Scalar::Util qw(blessed dualvar isdual readonly refaddr reftype
  tainted weaken isweak isvstring looks_like_number
  set_prototype);
use Encode::Locale;
use Hash::Merge qw( merge );
use Spreadsheet::WriteExcel;
use POSIX qw(strftime);
use File::Basename;

#use Data::Walker qw(:direct);

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

    my $header_fields = split_fields_by_new_line( $header_and_job->{header} );
    my $name_and_body = get_name_and_body( $header_and_job->{job} );
    debug( 1, $header_fields );
    my $ref_array_dsrecords = parse_records( $name_and_body->{job_body} );
    my $rich_records        = enrich_records($ref_array_dsrecords);
    my $orchestrate_code    = get_orchestrate_code($rich_records);
    my $parsed_dsx          = parse_orchestrate_body($orchestrate_code);
    my $links               = reformat_links($parsed_dsx);
    my $direction           = 'end';
    my ($lines) = fill_way_and_links( $links, $direction );

    #итак, все рассичтали, можно рисовать в excel
    make_excel_and_fill_header( $file_name, $header_fields );
    return $header_and_job;
}

sub make_excel_and_fill_header {
    my ( $file_name, $header_fields ) = @_;

    $file_name = basename( $file_name, ".dsx" );
    my $workbook =
      Spreadsheet::WriteExcel->new( $header_fields->{ToolInstanceID} . '_ON_'
          . $header_fields->{ServerName} . '_'
          . $file_name
          . '.xls' );
    set_excel_properties($workbook);

    # Add some worksheets
    my $revision_history = $workbook->add_worksheet("Revision_History");
    add_write_handler_autofit($revision_history);    #begin_autofit
    my $ref_formats = set_excel_formats($workbook);
    $revision_history->activate();
    fill_excel_header( $ref_formats, $revision_history, $header_fields );

    #my $i = 0;
    #for my $job_pop (@jobs_properties) {
    fill_excel_body( $ref_formats, $workbook );

    #$i++;
    #}
    $revision_history->activate();
    autofit_columns($revision_history);              #end_autofit

    # Run the autofit after you have finished writing strings to the workbook.

}

#
# New subroutine "fill_excel_body" extracted - Wed Nov 5 09:58:42 2014.
#
sub fill_excel_body {
    my $ref_formats = shift;
    my $workbook    = shift;

    fill_rev_history( $ref_formats, $workbook );

# my $curr_job_end =     make_curr_job($job_pop, $ref_formats, $workbook, $i, '2');
# my %job_and_formats_end;
# @job_and_formats_end{'ref_formats', 'curr_job', 'job_pop'} =
# ($ref_formats, $curr_job_end, $job_pop);
# my $lines = fill_excel_stages(\%job_and_formats_end, 'end');

    # my $mapping_sheet =
    # make_mapping_job(\%job_and_formats_end, $job_pop, $ref_formats,
    # $workbook, $i, 'mapping', $lines);
    # autofit_columns($curr_job_end);

    #    autofit_columns($mapping_sheet);

    # dump_in_html(\%job_and_formats_start);
}

sub fill_rev_history {
    my ( $ref_formats, $workbook ) = @_;
    $revision_history->write( 5 + $i, 5, $i,  $ref_formats->{rows_fmt} );
    $revision_history->write( 5 + $i, 6, "0", $ref_formats->{rows_fmt} );
    $revision_history->write_url(
        5 + $i, 7,
        'internal:' . substr( $job_pop->{JobName}, -28 ) . '_' . $num . '!A2',
        $ref_formats->{url_format},
        $job_pop->{JobName}
    );
    $revision_history->write( 5 + $i, 8, $job_pop->{JobDesc},
        $ref_formats->{rows_fmt} );
}

###############################################################################
#
# Functions used for Autofit.
#
###############################################################################
###############################################################################
#
# Adjust the column widths to fit the longest string in the column.
#
sub autofit_columns {
    my $worksheet = shift;
    my $col       = 0;
    for my $width ( @{ $worksheet->{__col_widths} } ) {
        $worksheet->set_column( $col, $col, $width ) if $width;
        $col++;
    }
}
###############################################################################
#
# The following function is a callback that was added via add_write_handler()
# above. It modifies the write() function so that it stores the maximum
# unwrapped width of a string in a column.
#
sub store_string_widths {
    my $worksheet = shift;
    my $col       = $_[1];
    my $token     = $_[2];

    # Ignore some tokens that we aren't interested in.
    return if not defined $token;       # Ignore undefs.
    return if $token eq '';             # Ignore blank cells.
    return if ref $token eq 'ARRAY';    # Ignore array refs.
    return if $token =~ /^=/;           # Ignore formula

    # Ignore numbers
    return
      if $token =~ /^([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/;

    # Ignore various internal and external hyperlinks. In a real scenario
    # you may wish to track the length of the optional strings used with
    # urls.
    return if $token =~ m{^[fh]tt?ps?://};
    return if $token =~ m{^mailto:};
    return if $token =~ m{^(?:in|ex)ternal:};

    # We store the string width as data in the Worksheet object. We use
    # a double underscore key name to avoid conflicts with future names.
    #
    my $old_width    = $worksheet->{__col_widths}->[$col];
    my $string_width = string_width($token);
    if ( not defined $old_width or $string_width > $old_width ) {

        # You may wish to set a minimum column width as follows.
        #return undef if $string_width < 10;
        $worksheet->{__col_widths}->[$col] = $string_width;
    }

    # Return control to write();
    return undef;
}
###############################################################################
#
# Very simple conversion between string length and string width for Arial 10.
# See below for a more sophisticated method.
#
sub string_width {
    return 0.9 * length $_[0];

    #return 1.1 * length $_[0];
}

#
# New subroutine "set_excel_properties" extracted - Wed Nov 5 09:44:48 2014.
#
sub set_excel_properties {
    my $workbook = shift;
    $workbook->set_properties(
        title    => 'Mapping for Reengineering',
        subject  => 'Generated from Datastage',
        author   => 'Nikolay Mishin',
        manager  => '',
        company  => '',
        category => 'mapping',
        keywords => 'mapping, perl, automation',
        comments => 'Автосгенерированный Excel файл',

        # status => 'В Работе',
    );
}

#
# New subroutine "set_excel_formats" extracted - Wed Nov 5 09:47:05 2014.
#
sub set_excel_formats {
    my $workbook = shift;

    # Add a Format
    my $heading = $workbook->add_format(
        align    => 'left',
        bold     => 1,
        border   => 2,
        bg_color => 'silver'
    );

    # size => 20,
    my $rows_fmt = $workbook->add_format( align => 'left', border => 1 );

    # $rows_fmt->set_text_wrap();
    my $date_fmt = $workbook->add_format(
        align      => 'left',
        border     => 1,
        num_format => 'mm.dd.yyyy'
    );
    my $num_fmt = $workbook->add_format(
        align      => 'left',
        border     => 1,
        num_format => '0.0'
    );
    my $url_format = $workbook->add_format(
        color     => 'blue',
        underline => 1,
    );
    my $sql_fmt = $workbook->add_format();
    $sql_fmt->set_text_wrap();
    $sql_fmt->set_size(8);
    $sql_fmt->set_font('Arial Narrow');
    $sql_fmt->set_align('bottom');
    $workbook->set_custom_color( 40, 141, 180, 226 );
    my $map_fmt = $workbook->add_format(
        bold     => 1,
        border   => 2,
        bg_color => 40,
    );
    my $acca_color = $workbook->set_custom_color( 40, 230, 230, 230 )
      ;    #light grey used in ACCA template

# $workbook->set_custom_color(40, 230,  230,  230); # light grey used in ACCA template
    my $light_orange = $workbook->set_custom_color( 43, 255, 226, 171 );
    my $ligth_yellow = $workbook->set_custom_color( 42, 255, 255, 153 );
    my $light_purple = $workbook->set_custom_color( 41, 225, 204, 255 );
    my $light_green  = $workbook->set_custom_color( 44, 204, 255, 153 );
    my $target_field_fmt = $workbook->add_format();
    $target_field_fmt->copy($heading);

    $target_field_fmt->set_size(11);
    $target_field_fmt->set_font('Calibri');
    $target_field_fmt->set_text_wrap();

    # $target_field_fmt->set_align('bottom');

    # $target_field_fmt->set_align('center');
    $target_field_fmt->set_bg_color($light_green);
    my $source_field_fmt = $workbook->add_format();
    $source_field_fmt->copy($target_field_fmt);
    $source_field_fmt->set_bg_color($ligth_yellow);

    # my $fm_grey = $workbook->add_format();
    # $fm_grey->copy($target_field_fmt);
    # $source_field_fmt->set_bg_color($ligth_yellow);

    my %formats;
    my $grey_color = $workbook->set_custom_color( 45, 128, 128, 128 );
    my $fm_grey =
      add_fmt_with_color( $workbook, $target_field_fmt, $grey_color );
    my $purple_color = $workbook->set_custom_color( 46, 204, 192, 218 );
    $formats{fm_purple} =
      add_fmt_with_color( $workbook, $target_field_fmt, $purple_color );

    my $light_blue_color = $workbook->set_custom_color( 47, 183, 222, 222 );
    $formats{fm_light_blue} =
      add_fmt_with_color( $workbook, $target_field_fmt, $light_blue_color );

    my $green_color = $workbook->set_custom_color( 48, 0, 176, 80 );
    $formats{fm_green} =
      add_fmt_with_color( $workbook, $target_field_fmt, $green_color );

    # $hs_name_frmt->set_bg_color($acca_color);

    @formats{
        'date_fmt', 'heading',          'num_fmt',
        'rows_fmt', 'url_format',       'sql_fmt',
        'map_fmt',  'target_field_fmt', 'source_field_fmt',
        'fm_grey'
      }
      = (
        $date_fmt, $heading,          $num_fmt,
        $rows_fmt, $url_format,       $sql_fmt,
        $map_fmt,  $target_field_fmt, $source_field_fmt,
        $fm_grey
      );

#my @formats=( $date_fmt, $heading, $num_fmt, $rows_fmt, $url_format,$sql_fmt,$map_fmt );
    return \%formats;
}

sub add_fmt_with_color {
    my ( $workbook, $target_field_fmt, $color ) = @_;
    my $fm = $workbook->add_format();
    $fm->copy($target_field_fmt);
    $fm->set_bg_color($color);
    return $fm;
}

#
# New subroutine "add_write_handler_autofit" extracted - Wed Nov 5 09:49:47 2014.
#
sub add_write_handler_autofit {
    my $sheet = shift;
###############################################################################
   #
   # Add a handler to store the width of the longest string written to a column.
   # We use the stored width to simulate an autofit of the column widths.
   #
   # You should do this for every worksheet you want to autofit.
   #
    $sheet->add_write_handler( qr[\w], \&store_string_widths );
}

#
# New subroutine "fill_excel_header" extracted - Wed Nov 5 09:54:20 2014.
#
sub fill_excel_header {
    my $ref_formats      = shift;
    my $revision_history = shift;
    my $head_prop        = shift;
    my $date             = strftime "%d.%m.%Y", localtime;
    $revision_history->write( 0, 0, "Date",        $ref_formats->{heading} );
    $revision_history->write( 0, 1, "Version",     $ref_formats->{heading} );
    $revision_history->write( 0, 2, "Description", $ref_formats->{heading} );
    $revision_history->write( 0, 3, "Author",      $ref_formats->{heading} );
    $revision_history->write( 1, 0, $date,         $ref_formats->{date_fmt} );
    $revision_history->write( 1, 1, "1.0",         $ref_formats->{num_fmt} );
    $revision_history->write(
        1, 2,
        "Initial version",
        $ref_formats->{rows_fmt}
    );
    $revision_history->write(
        1, 3,
        "Мишин Н.А.",
        $ref_formats->{rows_fmt}
    );
    $revision_history->write( 0, 5, "Project", $ref_formats->{heading} );
    $revision_history->write( 0, 6, "Server",  $ref_formats->{heading} );
    $revision_history->write(
        1, 5,
        $head_prop->{ToolInstanceID},
        $ref_formats->{rows_fmt}
    );
    $revision_history->write(
        1, 6,
        $head_prop->{ServerName},
        $ref_formats->{rows_fmt}
    );
    $revision_history->write( 4, 5, "Id",          $ref_formats->{heading} );
    $revision_history->write( 4, 6, "Parent_id",   $ref_formats->{heading} );
    $revision_history->write( 4, 7, "Sequence",    $ref_formats->{heading} );
    $revision_history->write( 4, 8, "Description", $ref_formats->{heading} );
}

sub enc_terminal {
    if (-t) {
        binmode( STDIN,  ":encoding(console_in)" );
        binmode( STDOUT, ":encoding(console_out)" );
        binmode( STDERR, ":encoding(console_out)" );
    }
}

#
# New subroutine "get_next_stage_for_link" extracted - Thu Nov 21 10:27:27 2014.
#
sub get_next_stage_for_link {
    my ( $links, $stage, $direction ) = @_;

    # input_links output_links
    # @{$stage->{$suffix}}
    my ( $out_suffix, $in_suffix ) = ( '', '' );
    if ( $direction eq 'start' ) {
        $out_suffix = 'output_links';
        $in_suffix  = 'input_links';
    }
    elsif ( $direction eq 'end' ) {
        $out_suffix = 'input_links';
        $in_suffix  = 'output_links';
    }

  #массив стадий, которые идут сразу за нашей
    my @next_stages = ();

#Выводим все выходные линки из текущей стадии
    for my $out_link_name ( @{ $stage->{$out_suffix} } ) {

        #идем по всем стадиям
        for my $loc_stage ( @{$links} ) {

#ищем входные линки совпадающие с нашим выходным
            for my $in_link_name ( @{ $loc_stage->{$in_suffix} } ) {
                if ( $out_link_name eq $in_link_name ) {

# say "\nЛинки совпали, ура!!!\n\n";
# say "$out_link_name in $stage->{stage_name} eq $in_link_name in $loc_stage->{stage_name}";
                    push @next_stages, $loc_stage;
                }
            }
        }
    }

    #считаем число стадий
    # my $cnt_of_next_stages=0+@next_stages;
    #возвращаем ссылку на массив стадий

    return \@next_stages;
}

sub check_for_dataset {
    my ( $cnt_links, $stage, $links_type ) = @_;

#также, если стейдж типа ds или это источник в виде базы данных 'pxbridge'
#у которого нет входящих линков для 1-го и выходящих для последнего
#точки приземления! (Андрей Бабуров)

    my $is_dataset = 'no';

    #кладем
    if ( $cnt_links == 1
        && substr( ${ $stage->{$links_type} }[0], -2 ) eq 'ds' )
    {
        $is_dataset = 'yes';
    }
    return $is_dataset;
}

sub check_for_started {
    my ( $cnt_links, $stage, $ref_start_stages_of, $is_dataset ) = @_;
    return (
        (
            exists $ref_start_stages_of->{ $stage->{operator_name} }
              && $cnt_links == 0
        )
          || ( $is_dataset eq 'yes' )
    );

}

sub reformat_links {
    my $parsed_dsx = shift;

  #my $link_and_fields = get_parsed_fields_by_link_name('L101', $parsed_fields);

    # print DumpTree( $parsed_dsx,   '$parsed_dsx' );
    # print DumpTree( $parsed_fields,   '$parsed_fields' );
    my $char = ':';

    my @only_links            = ();
    my @only_stages_and_links = ();
    my %stages_with_types     = ();
    foreach my $stage ( @{$parsed_dsx} ) {
        my %only_stages  = ();
        my @input_links  = ();
        my @output_links = ();
        $only_stages{stage_name}    = $stage->{stage_name};
        $only_stages{operator_name} = $stage->{operator_name};
        if ( $stage->{ins}->{in} eq 'yes' ) {
            for my $inputs ( @{ $stage->{ins}->{inputs} } ) {
                my %in_links = ();
                $in_links{link_name} = $inputs->{link_name};

                $in_links{is_param}      = 'no';
                $in_links{trans_name}    = $inputs->{trans_name};
                $in_links{operator_name} = $stage->{operator_name};
                $in_links{stage_name}    = $stage->{stage_name};
                $in_links{inout_type}    = $inputs->{inout_type};

                if ( $inputs->{is_param} eq 'yes' ) {
                    $in_links{is_param}         = 'yes';
                    $in_links{params}           = $inputs->{params};
                    $in_links{link_keep_fields} = $inputs->{link_keep_fields};

                    my $in_link_name = $inputs->{link_name};
                    my $in_real_link_name =
                      substr( $in_link_name,
                        index( $in_link_name, $char ) + 1 );

                }
                push @only_links,  \%in_links;
                push @input_links, $inputs->{link_name};
                $stages_with_types{ $inputs->{link_name} . '_'
                      . $inputs->{inout_type} } = \%in_links;
            }
        }
        $only_stages{input_links} = \@input_links;
        if ( $stage->{ins}->{out} eq 'yes' ) {
            for my $outputs ( @{ $stage->{ins}->{outputs} } ) {
                my %out_links = ();
                $out_links{link_name} = $outputs->{link_name};

                $out_links{is_param}      = 'no';
                $out_links{trans_name}    = $outputs->{trans_name};
                $out_links{operator_name} = $stage->{operator_name};
                $out_links{stage_name}    = $stage->{stage_name};
                $out_links{inout_type}    = $outputs->{inout_type};

                if ( $outputs->{is_param} eq 'yes' ) {
                    $out_links{is_param}         = 'yes';
                    $out_links{params}           = $outputs->{params};
                    $out_links{link_keep_fields} = $outputs->{link_keep_fields};

                    my $out_link_name = $outputs->{link_name};
                    my $out_real_link_name =
                      substr( $out_link_name,
                        index( $out_link_name, $char ) + 1 );

                }
                push @only_links,   \%out_links;
                push @output_links, $outputs->{link_name};
                $stages_with_types{ $outputs->{link_name} . '_'
                      . $outputs->{inout_type} } = \%out_links;
            }
        }
        $only_stages{output_links} = \@output_links;
        push @only_stages_and_links, \%only_stages;
    }
    my %out_hash = ();
    $out_hash{only_links}            = \@only_links;
    $out_hash{only_stages_and_links} = \@only_stages_and_links;
    $out_hash{stages_with_types}     = \%stages_with_types;
    my %cnt_links;
    for (@only_links) {
        $cnt_links{ $_->{link_name} . '_' . $_->{inout_type} }++;
    }

    # print DumpTree(\%out_hash, '\%out_hash');
    return \@only_stages_and_links;
}

#
# New subroutine "fill_excel_stages_and_links" extracted - Wed Nov 5 16:12:45 2014.
#

sub fill_way_and_links {
    my ( $links, $direction ) = @_;

    # my $links        = $all->{job_pop}->{only_links}->{only_stages_and_links};
    my @start_stages    = qw/copy pxbridge import/;
    my %start_stages_of = map { $_ => 1 } @start_stages;
    my $max             = 0;
    my $links_type = ( $direction eq 'start' ) ? 'input_links' : 'output_links';
    my %start_stages_name = ();
    my %a_few_stages      = ();
    my $cnt_stages        = 0 + @{$links};

    #    say "number of links: $cnt_stages";
    #хэш стейджей с объектами
    my %stages_body;
    for my $stage ( @{$links} ) {
        $stages_body{ $stage->{stage_name} } = $stage;
        my $cnt_links = 0 + @{ $stage->{$links_type} };

        my $is_dataset = check_for_dataset( $cnt_links, $stage, $links_type );
        my $is_started_links =
          check_for_started( $cnt_links, $stage, \%start_stages_of,
            $is_dataset );

        if ($is_started_links) {

         #находим все начальные линки,их имена!!!
            $a_few_stages{ $stage->{stage_name} }++;
        }
        my %link_collection = ();
        for my $direction ( 'start', 'end' ) {
            my $assoc_stages =
              get_next_stage_for_link( $links, $stage, $direction );
            $link_collection{$direction} = $assoc_stages;
        }
        $start_stages_name{ $stage->{stage_name} } = \%link_collection;
    }
    my ($lines) =
      calculate_right_way_for_stages( $direction, $links, \%a_few_stages,
        \%start_stages_name );

    #my %for_draw = ();
    # @for_draw{'all', 'orig_col', 'j', 'lines', 'links'} =
    #($all, $orig_col, $j, $lines, $links);

# @for_draw{'all', 'orig_col', 'j', 'lines', 'links'} =  ($all, $orig_col, $j + $max, $lines, $links);

    # ($max, $col) = draw_way_in_excel(\%for_draw);

    # $j = $j + 4 + $max;
    return $lines;    #($max, $lines);    #$j;
}

#
# New subroutine "calculate_right_way_for_stages" extracted - Mon Dec  1 01:36:32 2014.
#
sub calculate_right_way_for_stages {
    my $direction = shift;
    my $links     = shift;

    #my $col       = shift;

    # my $orig_col              = shift;
    # my $j                     = shift;
    my $ref_a_few_stages      = shift;
    my $ref_start_stages_name = shift;

    # print DumpTree(\%start_stages_name, '@$start_stages_name');

    # p %start_stages_name;

    #число стейджей всего:
    my $cnt_ctages = 0 + @{$links};

    #say "number of stages: $cnt_ctages";

#$cnt_ctages - это максимальное число вертикальных уровней или столбцов!!!

    #строим нашу цепочку без рекурсии!!
    #
    enc_terminal();
    my %lines = ();
    foreach my $few_stage ( sort keys %{$ref_a_few_stages} ) {
        $lines{$few_stage}++;
        my @elements   = ();
        my @levels     = ();
        my %in_already = ();
        for ( my $i = 0 ; $i < $cnt_ctages ; $i++ ) {
            my %stages_in_level    = ();
            my %collect_stages     = ();
            my $ref_collect_stages = \%collect_stages;

            #print "$i\n";
            if ( $i == 0 ) {
                $collect_stages{$few_stage} = 1;
                $in_already{$few_stage}++;
                push @levels, \%collect_stages;

         #say "Первый элемент: @{[ sort keys %collect_stages ]}\n";
                my $ref_0_stages =
                  get_next_stage_in_hash( $few_stage, $ref_start_stages_name,
                    $direction );
                push @levels, $ref_0_stages;
                foreach my $stg ( keys %{$ref_0_stages} ) {
                    $in_already{$stg}++;
                }

        #say "Второй элемент: @{[ sort keys %{$ref_0_stages} ]}\n";
            }
            elsif ( $i > 1 ) {
                my $prev_stages = $levels[ $i - 1 ];
                foreach my $prev_stage ( sort keys %{$prev_stages} ) {
                    my $ref_stages = get_next_stage_in_hash( $prev_stage,
                        $ref_start_stages_name, $direction );
                    $ref_collect_stages =
                      merge( $ref_collect_stages, $ref_stages );   #$ref_stages;

                }
                my %hash_for_check = %{$ref_collect_stages};

#проверяем получившийся хэш на стейджи, которые уже были
                foreach my $stg2 ( keys %hash_for_check ) {
                    if ( defined $in_already{$stg2} ) {
                        delete $hash_for_check{$stg2};
                    }

                }

                $ref_collect_stages = \%hash_for_check;
                if ( !keys %{$ref_collect_stages} ) {
                    last;
                }
                push @levels, $ref_collect_stages;    #\%collect_stages;
                foreach my $stg3 ( keys %{$ref_collect_stages} ) {
                    $in_already{$stg3}++;
                }

#               say "Третий элемент: @{[ sort keys %{$ref_collect_stages} ]}\n";
            }
        }
        $lines{$few_stage} = \@levels;
    }

    print DumpTree( \%lines, '$hash_ref_lines and direction: ' . $direction );
    return ( \%lines );
}

sub get_next_stage_in_hash {
    my ( $prev_stage, $ref_start_stages_name, $direction ) = @_;

#enc_terminal();
#say 'Для начала выясним, что у нас за переменные:';
#say 'Будем считать, что в хэше несколько стейджей,тогда пройдем по ним всем!!!:';
#say 'Предыдущий стейдж :' . $prev_stage;
    my $ref_link_array    = $ref_start_stages_name->{$prev_stage}->{$direction};
    my %stage_collections = ();
    for my $link ( @{$ref_link_array} ) {

        #       say $link->{stage_name};
        $stage_collections{ $link->{stage_name} }++;
    }
    return \%stage_collections;
}

sub parse_keep_fields {
    my $body_for_keep_fields = shift;
    $body_for_keep_fields =~ s/^\s+|\s+$//g;

    #p $body_for_keep_fields;
    my @fields = split /\s*,\s*/s, $body_for_keep_fields;
    return \@fields;
}

sub parse_fields {
    my $body_for_fields = shift;

    #p $body_for_fields;
    my @fields = ();
    my $field  = qr{
(?<field_name>\w+)
:
(?<is_null>not_nullable|nullable)\s
(?<field_type>.*?)
=
\g{field_name}
;
}xs;
    while ( $body_for_fields =~ m/$field/g ) {
        my %field_param = ();
        $field_param{field_name} = $+{field_name};
        $field_param{is_null}    = $+{is_null};
        $field_param{field_type} = $+{field_type};
        push @fields, \%field_param;
    }
    return \@fields;
}

sub parse_in_links {
    my ($body) = @_;
    my @links = ();

=pod
0< [] 'T100:L101.v'
1< [] 'T10:L11.v'
=cut

    my $link = qr{\d+
< (?:\||)
\s \[
(?<link_fields>
.*?
)
\]
\s
'
(?:
(?<link_name>
(?<trans_name>\w+):
\w+)
.v
|
\[.*?\]	
(?<link_name>
\w+.ds
)
)'
}xs;
    while ( $body =~ m/$link/g ) {
        my %link_param = ();
        $link_param{link_name}  = $+{link_name};
        $link_param{link_type}  = $+{link_fields};
        $link_param{inout_type} = 'input_links';

        # 'input_links'
        #$link_param{link_type} = $+{link_type};
        $link_param{trans_name} = $+{trans_name}
          if defined $+{trans_name};
        $link_param{is_param} = 'no';
        if ( defined $+{link_fields} )

          #if ( length( $link_param{link_type} ) >= 6
          #&& substr( $link_param{link_type}, 0, 6 ) eq 'modify' )
        {
            $link_param{is_param} = 'yes';
            $link_param{params}   = parse_fields( $+{link_fields} );
            $link_param{link_keep_fields} =
              parse_keep_fields( $+{link_keep_fields} )
              if defined $+{link_keep_fields};
        }
        push @links, \%link_param;
    }

    #print "\n\n Debug in_links!!! \n\n";
    #p $body;
    #p @links;
    return \@links;
}

sub parse_out_links {
    my ($body) = @_;
    my @links = ();

=pod
## General options
[ident('T108'); jobmon_ident('T108')]
## Inputs
0< [] 'T107:L107.v'
## Outputs
0> [] 'T108:L108.v'
1> [] 'T108:L_DBG01.v'
;
## General options
[ident('T199'); jobmon_ident('T199')]
## Inputs
0< [] 'LJ108:L109.v'
## Outputs
0> [] 'T199:INS.v'
1> [] 'T199:UPD.v'
;
=cut

    my $link = qr{\d+
(?:<|>)
(?:\||)
\s
\[
(?<link_type>
(?:
modify\s\(
(?:
(?<link_fields>
.*?;|.*?
)
)\n
keep
(?<link_keep_fields>
.*?
)
;
.*?
\)
)
|.*?
)
\]
\s
'
(?:
(?<link_name>
(?<trans_name>\w+):
\w+)
.v
|
\[.*?\]	
(?<link_name>
\w+.ds
)
)'
}xs;
    while ( $body =~ m/$link/g ) {
        my %link_param = ();
        $link_param{link_name}  = $+{link_name};
        $link_param{link_type}  = $+{link_fields};
        $link_param{inout_type} = 'output_links';

        #$link_param{link_type} = $+{link_type};
        $link_param{trans_name} = $+{trans_name}
          if defined $+{trans_name};
        $link_param{is_param} = 'no';
        if ( defined $+{link_fields} )

          #if ( length( $link_param{link_type} ) >= 6
          #&& substr( $link_param{link_type}, 0, 6 ) eq 'modify' )
        {
            $link_param{is_param} = 'yes';
            $link_param{params}   = parse_fields( $+{link_fields} );
            $link_param{link_keep_fields} =
              parse_keep_fields( $+{link_keep_fields} )
              if defined $+{link_keep_fields};
        }
        push @links, \%link_param;
    }

    # print "\n\n Debug out_links!!! \n\n";
    #p $body;
    #p @links;
    return \@links;
}

sub parse_stage_body {
    my ($stage_body) = @_;
    my %outs;
    my $inputs_rx  = qr{## Inputs\n(?<inputs_body>.*?)(?:#|^;$)}sm;
    my $outputs_rx = qr{## Outputs\n(?<outputs_body>.*?)^;$}sm;

=pod
## General options
[ident('LKUP101'); jobmon_ident('LKUP101')]
## Inputs
0< [] 'T100:L101.v'
1< [] 'T10:L11.v'
## Outputs
=cut

    my ( $inputs, $outputs ) = ( '', '' );
    $outs{in}   = 'no';
    $outs{out}  = 'no';
    $outs{body} = $stage_body;
    if ( $stage_body =~ $inputs_rx ) {
        $outs{inputs} = parse_in_links( $+{inputs_body} );
        $outs{in}     = 'yes';
    }
    if ( $stage_body =~ $outputs_rx ) {
        $outs{outputs} = parse_out_links( $+{outputs_body} );
        $outs{out}     = 'yes';
    }
    return \%outs;
}

sub make_orchestrate_regexp {

    my $ORCHESTRATE_BODY_RX = qr{
(?<stage_body>
\#\#\#\#[ ]STAGE:[ ](?<stage_name>\w+)[\n]
\#\#[ ]Operator[\n]
(?<operator_name>\w+)[\n]
.*?
[\n]
;
)
}sxm;
    return $ORCHESTRATE_BODY_RX;
}

sub parse_orchestrate_body {
    my $data                = shift;
    my $ORCHESTRATE_BODY_RX = make_orchestrate_regexp();
    local $/ = '';
    my @parsed_dsx = ();
    while ( $data =~ m/$ORCHESTRATE_BODY_RX/xsg ) {
        my %stage = ();
        my $ins   = parse_stage_body( $+{stage_body} );
        $stage{ins}           = $ins;
        $stage{stage_name}    = $+{stage_name};
        $stage{operator_name} = $+{operator_name};
        push @parsed_dsx, \%stage;
    }
    return \@parsed_dsx;
}

sub get_orchestrate_code {
    my $rich_records = shift;
    my $rec;
    my $Identifier = 'ROOT';
    my $seach_node = 'OrchestrateCode';

    my $curr_ref_array;

    for my $rec1 ( @{$rich_records} ) {
        my $loc_identifier = $rec1->{'fields'}->{'Identifier'};
        if ( defined $loc_identifier && $loc_identifier eq $Identifier ) {
            $curr_ref_array = $rec1;
        }
    }
    my $orch_code = $curr_ref_array->{'fields'}->{$seach_node};
    return $orch_code;
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
    my %fields_and_values = ();

    while (
        $curr_record =~ m/
(?<name>\w+)[ ]"(?<value>.*?)(?<!\\)"|
((?<name2>\w+)[ ]\Q=+=+=+=\E
(?<value2>.*?)
\Q=+=+=+=\E)
        /xsg
      )
    {
        my ( $value, $name ) = ( '', '' );
        if ( defined $+{name} ) {
            $name  = $+{name};
            $value = $+{value};
        }
        elsif ( defined $+{name2} ) {
            $name  = $+{name2};
            $value = $+{value2};
        }
        $fields_and_values{$name} = $value;
    }
    return \%fields_and_values;
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

