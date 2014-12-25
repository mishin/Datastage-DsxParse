package Datastage::DsxParse;
use 5.010;
use utf8;
use strict;
use warnings;
use Data::TreeDumper;
use Data::Dumper;
use File::Slurp qw(write_file read_file);
use Encode::Locale;
use Hash::Merge qw( merge );
use Spreadsheet::WriteExcel;
use POSIX qw(strftime);
use File::Basename;
use List::Util qw(first max maxstr min minstr reduce shuffle sum);
use Scalar::Util qw/reftype/;
use List::MoreUtils qw(any);

# use Data::Dumper::Simple;

#для отладки
#use Devel::ebug;

our $VERSION = "0.01";
use Sub::Exporter -setup => {
    exports => [
        qw/
          debug
          parse_dsx
          split_by_header_and_job
          split_fields_by_new_line
          get_name_and_body
          /
    ],
};

sub parse_dsx {
    my ($file_name)    = @_;
    my $data           = read_file($file_name);
    my $header_and_job = split_by_header_and_job($data);

    my $header_fields = split_fields_by_new_line($header_and_job->{header});
    my $name_and_body = get_name_and_body($header_and_job->{job});

    #debug( 1, $header_fields );
    my $ref_array_dsrecords = parse_records($name_and_body->{job_body});
    my $rich_records        = enrich_records($ref_array_dsrecords);
    my $orchestrate_code =
      get_orchestrate_code($rich_records, 'OrchestrateCode');

    # say '14:40';
    my ($parsed_dsx, $links, $direction, $lines);
    if (defined $orchestrate_code) {
        $parsed_dsx = parse_orchestrate_body($orchestrate_code);
        $links      = reformat_links($parsed_dsx);

        $direction = 'end';
        $lines = fill_way_and_links($links, $direction);

        # debug(1, $lines);
    }
    my %job_prop = ();
    @job_prop{
        'header_and_job', 'header_fields', 'rich_records',
        'parsed_dsx',     'links',         'direction',
        'lines'
      }
      = (
        $header_and_job, $header_fields, $rich_records, $parsed_dsx, $links,
        $direction, $lines
      );

    # 'header_and_job', 'header_fields',
    # $header_and_job, $header_fields,
    # 'parsed_dsx',$parsed_dsx,
    # debug (1,\%job_prop);
    my %deb = ();
    @deb{'$links', '$lines'} = ($links, $lines);

  #итак, все рассичтали, можно рисовать в excel
    my $debug_variable =
      make_excel_and_fill_header($file_name, $header_fields, \%job_prop);
    return \%deb;    #$links $lines;#$debug_variable;    #$header_and_job;
}

sub make_excel_and_fill_header {
    my ($file_name, $header_fields, $job_prop) = @_;

    $file_name = basename($file_name, ".dsx");

    # my $dir_name = dirname($file_name);$dir_name.'\'.
    my $workbook =
      Spreadsheet::WriteExcel->new($header_fields->{ToolInstanceID} . '_ON_'
          . $header_fields->{ServerName} . '_'
          . $file_name
          . '.xls');
    set_excel_properties($workbook);

    # Add some worksheets
    my $revision_history = $workbook->add_worksheet("Revision_History");
    add_write_handler_autofit($revision_history);    #begin_autofit
    my $ref_formats = set_excel_formats($workbook);
    $revision_history->activate();
    fill_excel_header($ref_formats, $revision_history, $header_fields);

    #my $i = 0;
    #for my $job_pop (@jobs_properties) {
    my %param_fields = ();
    @param_fields{'job_prop', 'ref_formats', 'workbook'} =
      ($job_prop, $ref_formats, $workbook);
    my $debug_variable = fill_excel_body(\%param_fields);

    #$i++;
    #}
    $revision_history->activate();
    autofit_columns($revision_history);    #end_autofit
    return $debug_variable;

    # Run the autofit after you have finished writing strings to the workbook.

}

#
# New subroutine "fill_excel_body" extracted - Wed Nov 5 09:58:42 2014.
#
sub fill_excel_body {
    my $param_fields = shift;

    # my $workbook    = shift;

    fill_rev_history($param_fields);

# my $curr_job_end =     make_curr_job($job_pop, $ref_formats, $workbook, $i, '2');
# my %job_and_formats_end;
# @job_and_formats_end{'ref_formats', 'curr_job', 'job_pop'} =
# ($ref_formats, $curr_job_end, $job_pop);
# my $lines = fill_excel_stages(\%job_and_formats_end, 'end');

    my ($mapping_sheet, $debug_variable) = make_mapping_job($param_fields);

    # autofit_columns($curr_job_end);

    autofit_columns($mapping_sheet);
    return $debug_variable;

    # dump_in_html(\%job_and_formats_start);
}

sub get_worksheet_by_name {
    my ($workbook, $sheet_name) = @_;
    my $curr_sheet;
    foreach my $worksheet ($workbook->sheets()) {
        if ($worksheet->get_name() eq $sheet_name) {
            $curr_sheet = $worksheet;
        }
    }
    return $curr_sheet;
}

sub make_mapping_job {
    my ($param_fields) = @_;

    # debug(1, $param_fields);

    my $workbook      = $param_fields->{workbook};
    my $loc_hash_prop = $param_fields->{job_prop};
    my $lines         = $param_fields->{job_prop}->{lines};
    my $ref_formats   = $param_fields->{ref_formats};

    my $job_name =
      get_orchestrate_code($param_fields->{job_prop}->{rich_records}, 'Name');
    my $curr_job =
      $workbook->add_worksheet(substr($job_name, -20) . '_mapping');
    $curr_job->activate();
    add_write_handler_autofit($curr_job);
    my @deb_array                = ($lines);
    my %start_stages_for_mapping = ();
    for my $key (keys %{$lines}) {
        my @arr = @{$lines->{$key}};
        for my $stage_name (keys %{$arr[0]}) {
            $start_stages_for_mapping{$stage_name}++;
        }
    }

    my $ref_fields = get_header_values();
    for my $field (@{$ref_fields}) {
        $curr_job->write($field->{coord}, $field->{caption},
            $ref_formats->{$field->{format}});
    }

    $curr_job->write(
        'B4',
        $loc_hash_prop->{JobName},
        $ref_formats->{rows_fmt}
    );
    $curr_job->write(
        'B3',
        $loc_hash_prop->{JobName},
        $ref_formats->{rows_fmt}
    );

    my $j     = 3;
    my $col   = 7;
    my $links = $param_fields->{job_prop}->{links};
    my %db    = (
        'links'                      => $links,
        '\%start_stages_for_mapping' => \%start_stages_for_mapping
    );

    # debug(1, \%db);
    my @fill_excel            = ();
    my $new_number_of_records = 0;
    my $rec_fields            = 3;

    for my $final_stage_for_draw (keys %start_stages_for_mapping) {
        $rec_fields = $rec_fields + $new_number_of_records;

        # say $rec_fields;
        my $link_body =
          get_body_of_stage($param_fields, $final_stage_for_draw, $links);

        # say ' $final_stage_for_draw: ' . $final_stage_for_draw;

        #пишем в excel !!
        $curr_job->write_row('B' . $rec_fields, $link_body);

# my @fake_empty = ();
# $#fake_empty = 20;
# my $empty_line_coordination = @{$$link_body[0]} + 0;
# $curr_job->write_row('A' . ($rec_fields + $empty_line_coordination),       \@fake_empty, $ref_formats->{fm_green_empty});
        $new_number_of_records = @{$$link_body[0]} + 0;

        # say '$final_stage_for_draw: ' . $final_stage_for_draw;
    }

    my $stage_name = 'address_insert';
    my $debug_variable = get_stage($links, $stage_name);

    # address_insert.STRNAM
    #   (1, \@show_fields);
    return ($curr_job, $debug_variable);
}

sub get_stage {
    my ($links, $stage_name) = @_;
    my $stage_body;
    for my $loc_stage (@{$links}) {
        if ($loc_stage->{stage_name} eq $stage_name) {
            $stage_body = $loc_stage;
        }
    }
    return $stage_body;
}

#
# New subroutine "get_body_of_stage" extracted - Thu Nov 21 10:27:27 2014.
#
sub get_body_of_stage {
    my ($param_fields, $stage_name, $links) = @_;
    my $stage_body;

    # debug (1, $param_fields);

    #my $links=$param_fields;
    #идем по всем стадиям
    for my $loc_stage (@{$links}) {
        if ($loc_stage->{stage_name} eq $stage_name) {
            $stage_body = $loc_stage;
        }
    }

=pod

если оператор, то логика меняется
нужно извлекать параметр -file из dsx_parse

#################################################################
#### STAGE: CRZ_History
## Operator
export
## Operator options
-schema record
  {final_delim=end, delim='|', null_field='', quote=none}
(
  HST_DATE:nullable string[];
  HST_CODE:nullable string[];
  HST_NAME:nullable string[];
  Z_UID:string[max=36];
)
-file '[&"TempFilePathDS"]crz_history.txt'
-overwrite
-rejects continue

## General options
[ident('CRZ_History'); jobmon_ident('CRZ_History')]
## Inputs
0< [] 'Transformer_1:Hist.v'
;

в этом его отличие от

#################################################################
#### STAGE: Data_Set_RL1
## Operator
copy
## General options
[ident('Data_Set_RL1')]
## Inputs
0< [] 'Transformer_1:RL1.v'
## Outputs
0>| [ds] '[&"TempFilePathDS"]rl1.ds'
;
где датасет уже является оутпут линком!!
=cut

    # if ($stage_name eq 'ADR'){
    # print Dumper $stage_body;
    # }

    my $link_name = $stage_body->{'input_links'}[0];

    # print Dumper $stage_body;
    say 'link_name: ' . $link_name;

    my $xml_prop = get_xml_properties($param_fields, $stage_name);
    my $xml_fields = parse_xml_properties($xml_prop);

    #получаем схему и имя таблицы
    #или путь и имя файла или датасета
    my %table_comp = get_table_ds_file_name(
        $stage_body, $stage_name, $param_fields,
        $link_name,  $xml_prop,   $xml_fields
    );

    #итак собираем excel
    #
    # ServerName "SDBDS2"
    # ToolInstanceID "MASTER_for_CDB"
    #1.project
    my $header_flds = $param_fields->{job_prop}->{header_fields};
    my $project =
      $header_flds->{ServerName} . '/' . $header_flds->{ToolInstanceID};

    #2. Job - есть!!!
    my $job = get_job_name($param_fields->{job_prop}->{rich_records},
        'CJobDefn', 'Name');

    #3.ПРИЕМНИК ДАННЫХ или сервер
    my $server = $xml_fields->{Connection}->{DataSource}->{content};
    if (!defined $server) {
        $server = $table_comp{server};
    }

    #4.schema
    my $schema = $table_comp{schema};

    #5.table
    my $table_name = $table_comp{table_name};

    #sql поля есть
    my $sql_fields = get_sql_fields($param_fields, $link_name);

    #6.fields
    my $fields = get_source_sql_field($sql_fields, 'Name');

    # make_sql_fields_for_show($sql_fields);

    # say '$fields: ';
    # print Dumper $fields;

    #7.types
    my $types = get_sql_types($sql_fields, $fields);

    #8.Вхождение в ключ
    my $key = get_sql_keys($sql_fields, $fields);

    #9.Обязательность
    my $nullable = get_sql_mandatory($sql_fields, $fields);

    #10.Формула
    my $parsedderivation =
      get_source_sql_field($sql_fields, 'ParsedDerivation', $param_fields);

    # get_source_sql_field($sql_fields, 'ParsedDerivation');

    #10.Исходное поле
    my $sourcecolumn =
      get_source_sql_field_parsed($sql_fields, $param_fields, $stage_name);

#10.Если $sourcecolumn
#address_insert.STRNAM;address_insert.HOUSE;address_insert.CORP;address_insert.FLAT
#то нужно разделить строку на число источников

    #11.Описание
    my $descriptions = get_source_sql_field($sql_fields, 'Description');

    my @show_values = (
        $project,    $job,              $server,       $schema,
        $table_name, $fields,           $types,        $key,
        $nullable,   $parsedderivation, $sourcecolumn, $descriptions
    );
    my $big_array = make_data_4_show(\@show_values, $fields);

    # print Dumper $big_array;
    return $big_array;
}

sub get_source_sql_field_parsed {
    my ($sql_fields, $param_fields, $stage_name) = @_;
    my $field_name = 'SourceColumn';

# say 'get_source_sql_field_parsed!!';
    #$field_name='ParsedDerivation'
    my @sql_user_fiendly = ();
    for my $sql_field (@{$sql_fields}) {
        my $field_body = $sql_field->{$field_name};
        if (defined $field_body) {
            my ($cnt, $src_fields) = is_multiple_source($field_body);
            if ($cnt > 1) {
                for my $field (@{$src_fields}) {

#Если это поле источник, то заполняем каждое поле в отдельности
                    push @sql_user_fiendly, from_dsx_2_utf($field);
                    debug_parsed('1_' . $field_body,
                        $field, $stage_name, $param_fields);

                }
            }
            else {

#если источником является одно поле, то все падает сюда!
                push @sql_user_fiendly, from_dsx_2_utf($field_body);
                debug_parsed(
                    '2_' . $field_body, $field_body,
                    $stage_name,        $param_fields
                );

            }
        }
    }
    return \@sql_user_fiendly;
}

sub show_variable {
    my ($value, $name) = @_;
    my $fixed_length = 40;

    #say '#' x $fixed_length;
    my $string = "# DEBUG $name:  $value";
    $string .= ' ' x ($fixed_length - length($string) - 1);
    say $string. '#';
    say '#' x $fixed_length;

    #say '';
}

sub debug_show_field_and_link {
    my ($orig_fld, $orig_link) = @_;
    if (defined $orig_fld) {
        show_variable($orig_fld, '$orig_fld');
    }
    if (defined $orig_link) {
        show_variable($orig_link, '$orig_link');
    }
}

# my $lines     = $param_fields->{job_prop}->{lines};
# my $curr_line = $lines->{$stage_name};
# сложные случаи, где SourceColumn:=@INROWNUM будем рассматривать после того, как завершим дело с простыми случаями обычными l2.GR2198

sub debug_parsed {
    my $mark          = shift;
    my $field         = shift;
    my $stage_name    = shift;
    my $param_fields  = shift;
    my @deriv_collect = ();
    if (defined $field) {
        my $links = $param_fields->{job_prop}->{links};
        my ($orig_link, $orig_fld) = split(/[.]/, $field);

        # my $link_name = $stage_name . ':' . $orig_link;

        my $link_body =
          get_body_of_records($param_fields, $orig_link, 'CTrxOutput');
        show_parsed_constraint($link_body);
        show_variable($mark, '$mark_debug_parsed_' . $stage_name);

        my ($parse_and_source, $pars_deriv, $source_col) =
          calc_deriv($param_fields, $field);

        while (defined $source_col) {
            push @deriv_collect, $parse_and_source;
            ($parse_and_source, $pars_deriv, $source_col) =
              calc_deriv($param_fields, $source_col);
        }

        say Dumper \@deriv_collect;
    }
}

sub calc_deriv {
    my ($param_fields, $source_col) = @_;
    my ($loc_orig_link, $loc_orig_fld) = split(/[.]/, $source_col);
    my $parse_and_source =
      get_source_and_derivation($param_fields, $loc_orig_link, $loc_orig_fld,
        $source_col);
    my $pars_deriv = $parse_and_source->{$source_col}->{parsed_derivation};
    $source_col = $parse_and_source->{$source_col}->{source_column};
    return ($parse_and_source, $pars_deriv, $source_col);
}

sub get_deriv_from_all {
    my ($curr_line, $links, $orig_link, $param_fields, $link_name, $orig_fld,
        $parse_and_source, $field)
      = @_;
    my %check_strange_array = ();
    my @collect_derivations = ();

    # parsed_derivation','source_column
    for my $stage_hash (@{$curr_line}) {
        for my $loc_stage_name (keys %{$stage_hash}) {

#а, если стейджей несколько, что это не верно!!

#выведем линки, принадлежащие стейджу здесь мы по сути ищем имя линка!!
            my $stage_and_links = get_stage($links, $loc_stage_name);
            my $loc_link_name = $loc_stage_name . ':' . $orig_link;
            for my $in_suffix (qw/input_links output_links/) {
                for my $in_link_name (@{$stage_and_links->{$in_suffix}}) {
                    if ($loc_link_name eq $in_link_name) {

                        if (defined $parse_and_source->{parsed_derivation}) {

          # say Dumper $parse_and_source;
          # say 'ParsedDerivation: ' . $parse_and_source->{parsed_derivation};

# сложные случаи, где SourceColumn:=@INROWNUM будем рассматривать после того, как завершим дело с простыми случаями обычными l2.GR2198
                            if (defined $parse_and_source->{source_column}) {
                                my $loc_deriv;

         # say 'SourceColumn:' . $parse_and_source->{source_column}          ;
                                my ($loc_orig_link, $loc_orig_fld) =
                                  split(/[.]/,
                                    $parse_and_source->{source_column});

                                # say '$loc_stage_name: ' . $loc_stage_name;
                                say
                                  'линки совпали, дампим!! выводим 2-й уровень ссылок на переменные';

# debug_show_field_and_link($loc_orig_fld,                    $loc_orig_link);
                                my $loc_link_name =
                                  $loc_stage_name . ':' . $loc_orig_link;

# show_variable( $loc_link_name,                                    '$loc_link_name' );
# show_variable( $loc_orig_fld, '$loc_orig_fld' );
# show_variable( $loc_stage_name,                                    '$loc_stage_name' );
                                $loc_deriv = calculate_derivations(
                                    $param_fields,
                                    $loc_link_name,
                                    $loc_orig_fld,
                                    $parse_and_source,
                                    $loc_stage_name,
                                    $parse_and_source->{source_column}
                                );
                                push @collect_derivations, $loc_deriv;
                            }
                        }

#итак для данной линки                        # print Dumper $stage_and_links;                        # push @next_stages, $loc_stage;
                    }
                }
            }

# $check_strange_array{$loc_stage_name} =              calculate_derivations( $param_fields, $link_name, $orig_fld,                $parse_and_source, $loc_stage_name ,$field );
        }
    }

    # say '\@collect_derivations';
    # say Dumper \@collect_derivations;
    return \%check_strange_array;
}

sub show_parsed_constraint {
    my ($link_body) = @_;

    # my $param = 'ParsedConstraint';
    get_parsed_constraint_from_link($link_body);
    my $constraint = $link_body->{fields}->{'ParsedConstraint'};
    if (defined $constraint) {
        say "debug \$constraint=$constraint";
    }

# say
# "debug [$mark] !! \$field: [$orig_fld] \$stage_name: [$stage_name] link: [$orig_link]";
# say '';

}

# Name "L105" L104.FILE_VERSION  GBC - GlowByteConsulting

=pod
     Identifier "V0S26P3"
      OLEType "CTrxOutput"
      Readonly "0"
      Name "L104"
      Partner "V0S83|V0S83P5"
      Reject "0"
      ErrorPin "0"
      RowLimit "0"
      Columns "COutputColumn"
      
         Derivation "L103.FILE_VERSION"
         Group "0"
         ParsedDerivation "L103.FILE_VERSION"
         SourceColumn "L103.FILE_VERSION"
      
=cut    

sub calculate_derivations {
    my ($param_fields, $link_name, $orig_fld, $parse_and_source,
        $loc_stage_name, $field)
      = @_;

    # say 'calculate_derivations';
    my $derivations;
    my ($cnt, $src_fields) =
      is_multiple_source($parse_and_source->{$field}->{'source_column'});
    if ($cnt > 1) {
        $derivations =
          get_multiple_derivation($src_fields, $param_fields, $loc_stage_name,
            $orig_fld, $field);
        say 'calculate_derivations cnt>1';

    }
    else {
        $derivations =
          get_source_and_derivation($param_fields, $link_name, $orig_fld,
            $field);
        say 'calculate_derivations cnt=1';
    }
    return $derivations;
}

sub get_multiple_derivation {
    my ($src_fields, $param_fields, $loc_stage_name, $orig_fld, $field) = @_;
    say 'get_multiple_derivation';
    my @fields_and_derivations = ();
    for my $ff (@{$src_fields}) {
        my ($loc_link, $loc_fld) = split(/[.]/, $ff);
        my $loc_link_name = $loc_stage_name . ':' . $loc_link;
        my $loc_parse_and_source =
          get_source_and_derivation($param_fields, $loc_link_name, $loc_fld,
            $ff);

        # my %loc_param = ();
        # @loc_param{ 'ff', 'loc_link', 'loc_fld', 'loc_parse_and_source' } =
        # ( $ff, $loc_link, $loc_fld, $loc_parse_and_source );
        push @fields_and_derivations, $loc_parse_and_source;    #\%loc_param;

        # $fields_and_derivations{$ff}=$loc_parse_and_source;
        # print Dumper $loc_parse_and_source;
    }

    # say Dumper \@fields_and_derivations;
    return \@fields_and_derivations;
}

sub get_source_and_derivation {
    my ($param_fields, $orig_link, $orig_fld, $field) = @_;

    # my $link_body =
    # get_parsed_fields_from_all($param_fields, $link_name, 'CTrxOutput');

    my $link_body =
      get_body_of_records($param_fields, $orig_link, 'CTrxOutput');

    my $fields               = get_parsed_any($orig_fld, $link_body);
    my $parsed_derivation    = $fields->{ParsedDerivation};
    my $source_column        = $fields->{SourceColumn};
    my %compact_construction = ();
    my %parse_and_source     = ();
    @parse_and_source{'parsed_derivation', 'source_column'} =
      ($parsed_derivation, $source_column);
    $compact_construction{$field} = \%parse_and_source;
    return \%compact_construction;
}

sub get_parsed_constraint_from_link {
    my ($link_body) = @_;

    # OLEType "CTrxOutput"
    # my $OLEType = 'CTrxOutput';    # qw/CTrxOutput CCustomOutput/;

    # #say 'Opps..';
    # my $link_body =
    # get_parsed_fields_from_all($param_fields, $link_name, $OLEType);
    # print DumpTree($link_body,           '$link_body');
    # print DumpTree($link_body->{fields}, '$link_body->{fields}');
    my $parsed_constraint = $link_body->{fields}->{ParsedConstraint};
    if (defined $parsed_constraint) {
        say '$parsed_constraint: ' . $parsed_constraint;
        return $parsed_constraint;

        # print DumpTree($link_body, '$link_body');

        # debug(1, $link_body);
    }
    else {
        return undef;
    }

    # my $sql_fields  = $link_body->{subrecord_body};
    # my @sql_records = ();
    # for my $rec ( @{ $link_body->{subrecord_body} } ) {
    # if ( defined $rec->{SqlType} ) {
    # push @sql_records, $rec;
    # }
    # }
    # return \@sql_records;

}

sub get_parsed_any {
    my ($field, $link_body) = @_;

    my $sql_fields  = $link_body->{subrecord_body};
    my @sql_records = ();
    my $curr_record;
    for my $rec (@{$link_body->{subrecord_body}}) {
        if (defined $rec->{SqlType} && $rec->{Name} eq $field) {
            $curr_record = $rec;
        }
    }

    return $curr_record;
}

# sub add_to_array

sub get_link_name_from_parsed {
    my $in_link_name = shift;    # 'l1.GR1576';
    my $in_real_link_name =
      substr($in_link_name, 0, index($in_link_name, '.'));
    return $in_real_link_name;
}

sub get_field_name_from_parsed {
    my $in_link_name = shift;    # 'l1.GR1576';
    my $in_real_link_name =
      substr($in_link_name, index($in_link_name, '.') + 1);
    return $in_real_link_name;
}

sub get_source_sql_field {
    my ($sql_fields, $field_name) = @_;
    my @sql_user_fiendly = ();
    for my $sql_field (@{$sql_fields}) {
        my ($cnt, $src_fields) =
          is_multiple_source($sql_field->{'SourceColumn'});
        if ($cnt > 1) {
            for my $field (@{$src_fields}) {
                if ($field_name eq 'SourceColumn') {
                    push @sql_user_fiendly, from_dsx_2_utf($field);
                }
                else {
                    push @sql_user_fiendly,
                      from_dsx_2_utf($sql_field->{$field_name});
                }
            }
        }
        else {
            push @sql_user_fiendly, from_dsx_2_utf($sql_field->{$field_name});
        }
    }
    return \@sql_user_fiendly;
}

sub get_sql_types {
    my ($sql_fields, $fields) = @_;
    my @sql_user_fiendly = ();
    for my $sql_field (@{$sql_fields}) {
        my $type =
          decode_sql_type($sql_field->{SqlType}, $sql_field->{Precision});
        my ($cnt, $src_fields) =
          is_multiple_source($sql_field->{'SourceColumn'});
        if ($cnt > 1) {
            for (@{$src_fields}) {
                push @sql_user_fiendly, $type;
            }
        }
        else {
            push @sql_user_fiendly, $type;
        }
    }
    return \@sql_user_fiendly;
}

sub make_sql_fields_for_show {
    my ($sql_fields) = @_;
    my @sql_user_fiendly = ();
    for my $sql_field (@{$sql_fields}) {

#address_insert.STRNAM;address_insert.HOUSE;address_insert.CORP;address_insert.FLAT Name
        my @src_fields = ();
        my $src_column = $sql_field->{'SourceColumn'};
        my $field_name = $sql_field->{'Name'};
        my ($cnt, $src_fields) = is_multiple_source($src_column);
        if ($cnt > 1) {
            say 'SourceColumn: ' . $src_column;
            for (@{$src_fields}) {
                push @sql_user_fiendly, $field_name;
            }
        }
        else {
            push @sql_user_fiendly, $field_name;
        }
    }

    # print Dumper \@sql_user_fiendly;
    return \@sql_user_fiendly;
}

# Identifier "V0S32"
# OLEType "CCustomStage"
# Readonly "0"
# Name "CRZ_History"
# NextID "2"
# InputPins "V0S32P1"
# StageType "PxSequentialFile"

sub get_ds_properties {
    my ($param_fields, $link_name) = @_;
    my $OLEType = 'CCustomInput';
    my $records =
      get_parsed_fields_from_all($param_fields, $link_name, $OLEType);

    my %d = ('$records' => $records, link_name => $link_name);

    # debug(1, \%d);
    # print DumpTree( \%d,   '\%d' );

    my $ds_name;
    my @ds_types = qw/dataset file/;
    for my $rec (@{$records->{subrecord_body}}) {
        if (any { $rec->{Name} eq $_ } @ds_types) {
            $ds_name = from_dsx_2_utf($rec->{Value});
        }
    }
    if (defined $ds_name) {
        $ds_name =~ s{(\\\(\d\)0|\\\(\d\))}{}g;
    }
    else {
        $ds_name = $link_name;
    }
    return $ds_name;
}

sub get_file_properties {
    my ($param_fields, $stage_name) = @_;
    my $parsed_dsx = $param_fields->{job_prop}->{parsed_dsx};
    my $file_name;
    for my $rec (@{$parsed_dsx}) {
        if ($rec->{stage_name} eq $stage_name) {
            $file_name =
              $rec->{'ins'}->{'operator_options'}
              ->{'-file'};    #$parsed_dsx->{operator_options} ;
        }
    }

    return $file_name;
}

sub get_type_file_or_ds_properties {
    my ($param_fields, $stage_name) = @_;
    my $parsed_dsx = $param_fields->{job_prop}->{parsed_dsx};
    my $type;
    for my $rec (@{$parsed_dsx}) {
        if ($rec->{stage_name} eq $stage_name) {
            $type = $rec->{'operator_name'};
        }
    }
    return $type;
}

sub make_data_4_show {
    my ($values_4_show, $fields) = @_;
    my @big_array = ();

    # my @entity_array = ();
    for my $entity (@{$values_4_show}) {
        my $reftype = reftype $entity;

 #это не ссылка, а простой скаляр или строка
        if (!defined $reftype) {
            my @entity_array = map {$entity} @{$fields};
            push @big_array, \@entity_array;
        }
        elsif ($reftype eq 'ARRAY') {
            push @big_array, $entity;
        }
    }

    return \@big_array;
}

sub get_sql_field {
    my ($sql_fields, $field_name) = @_;
    my @sql_user_fiendly = ();
    for my $sql_field (@{$sql_fields}) {

        my $src_column = $sql_field->{'SourceColumn'};
        my ($cnt, $src_fields) = is_multiple_source($src_column);
        if ($cnt > 1) {

            # say 'SourceColumn: ' . $src_column;
            for (@{$src_fields}) {
                push @sql_user_fiendly,
                  from_dsx_2_utf($sql_field->{$field_name});
            }
        }
        else {

            push @sql_user_fiendly, from_dsx_2_utf($sql_field->{$field_name});
        }
    }
    return \@sql_user_fiendly;
}

sub is_multiple_source {
    my ($src_column) = @_;
    my @src_fields = ();
    if (defined $src_column) {
        @src_fields = split(/;/, $src_column);
    }
    my $cnt = @src_fields + 0;
    return ($cnt, \@src_fields);
}

sub from_dsx_2_utf {
    my $string = shift;
    if (defined $string) {
        $string =~ s#\Q\(A)\E#\n#g;
        $string =~ s#\Q\(9)\E#\t#g;
        $string =~ s#\\([^(])#$1#g;
        $string =~ s#Searchable\? [YN]##g;
        $string =~ s#\\\((...)\)#chr(hex$1)#gsme;
        $string =~ s#\\\((....)\)#chr(hex$1)#gsme;
    }
    return $string;
}

sub double_slash_2_slash {
    my $string = shift;
    $string =~ s#\\\\#\\#g;
    return $string;
}

sub get_sql_mandatory {
    my ($sql_fields) = @_;
    my @sql_user_fiendly = ();
    for my $sql_field (@{$sql_fields}) {
        my $key = ($sql_field->{Nullable} == '1') ? 'НЕТ' : 'ДА';

        my $src_column = $sql_field->{'SourceColumn'};
        my ($cnt, $src_fields) = is_multiple_source($src_column);
        if ($cnt > 1) {

            # say 'SourceColumn: ' . $src_column;
            for (@{$src_fields}) {
                push @sql_user_fiendly, $key;
            }
        }
        else {
            push @sql_user_fiendly, $key;
        }
    }
    return \@sql_user_fiendly;
}

sub get_sql_keys {
    my ($sql_fields) = @_;
    my @sql_user_fiendly = ();
    for my $sql_field (@{$sql_fields}) {
        my $key = ($sql_field->{KeyPosition} == '1') ? 'ДА' : 'НЕТ';

        my $src_column = $sql_field->{'SourceColumn'};
        my ($cnt, $src_fields) = is_multiple_source($src_column);
        if ($cnt > 1) {

            # say 'SourceColumn: ' . $src_column;
            for (@{$src_fields}) {
                push @sql_user_fiendly, $key;
            }
        }
        else {

            push @sql_user_fiendly, $key;
        }
    }
    return \@sql_user_fiendly;
}

sub decode_sql_type {
    my $code       = shift;
    my $precicion  = shift;
    my $debug_info = shift;
    my %param_type;
    @param_type{1, 4, 9, 12, 3, 5, 6, 10, 13, 11, 8} = (
        'Char',    'Integer',   'Date',    'VarChar',
        'Decimal', 'SmallInt',  'Unnown6', 'Time',
        'VarChar', 'Timestamp', 'dfloat'
    );

    # CTNUMDOG=12, RESTRUCT_CNT=4, RESTRUCT_END_DATE=9', 'CTNUMDOG=13
    my $value = '';
    if (defined $code) {
        $value = $param_type{$code}
          or die
          "for code: $code we have not value \$debug_info: $debug_info";
    }
    my $sql_type = $value;
    if (defined $precicion && $precicion != 0 && $value ne 'Date') {
        $sql_type = $value . '(' . $precicion . ')';
    }
    return $sql_type;
}

sub get_job_name {
    my ($rich_records, $oletype, $field_name) = @_;    #shift;

    # my $rec;
    # my $Identifier = 'ROOT';

    # my $seach_node = 'OrchestrateCode';

    my $curr_ref_array;
    my $value;
    for my $rec1 (@{$rich_records}) {
        my $loc_ole_type = $rec1->{'fields'}->{'OLEType'};
        if ($loc_ole_type eq $oletype) {
            $value = $rec1->{'fields'}->{$field_name};
        }
    }

    # my $orch_code = $curr_ref_array->{'fields'}->{$seach_node};
    return $value;
}

sub get_table_name {
    my ($xml_field) = @_;
    my $table_name = $xml_field->{Usage}->{TableName}->{content};

    if (defined $table_name) {
        return uc($table_name);
    }

    # say 'sql: ';
    # print Dumper $xml_field->{Usage};
    my $sql = $xml_field->{Usage}->{SQL}->{UserDefinedSQL}->{Statements};

    # print 'show sql: '.from_dsx_2_utf($sql);
    if (defined $sql) {
        print 'show sql: ' . from_dsx_2_utf($sql);

        # print Dumper $xml_field->{Usage}->{SQL};
        return from_dsx_2_utf($sql);
    }
    return 'no';
}

sub parse_xml_properties {
    my ($xml) = @_;
    my $dom;

    # print Dumper \$xml;
    if (defined $xml) {
        use XML::Simple;
        $xml =~ s/UTF-16/UTF-8/;
        $dom = XMLin($xml);
    }
    return $dom;
}

#
# New subroutine "get_table_ds_file_name" extracted - Sat Dec 20 12:05:31 2014.
#
sub get_table_ds_file_name {
    my $stage_body   = shift;
    my $stage_name   = shift;
    my $param_fields = shift;
    my $link_name    = shift;
    my $xml_prop     = shift;
    my $xml_fields   = shift;

    my %table_comp = ();
    my ($table_name);

    if (defined $xml_prop) {

        $table_name = get_table_name($xml_fields);
        $table_name =~ /(?<schema>.*)[.](?<table_name>[^.]+)$/;
        %table_comp = %+;
    }
    else {

#Если это Dataset, то его название можно взять из Orcestrate кода!!! и берем его из output_links
        my $link_name_for_ds;

        my $type = get_type_file_or_ds_properties($param_fields, $stage_name);

        # say '$type: '.$type;
        # debug(1,$param_fields);

=pod
   BEGIN DSRECORD
      Identifier "V46S2"
      OLEType "CCustomStage"
      Readonly "0"
      Name "ROSBANKV2_CREDIT_tmp_xml"
      NextID "2"
      InputPins "V46S2P1"
      StageType "XMLOutputPX"
      AllowColumnMapping "0"
      Properties "CCustomProperty"
      BEGIN DSSUBRECORD
         Name "output_file"
         Value "#PS_BCE_CONNECTIONS.DIR_NR#ROSBANKV2_CREDIT_#LAST_DATE#_tmp.xml"
      END DSSUBRECORD
=cut      

        if ($type eq 'export') {
            $link_name_for_ds =
              get_file_properties($param_fields, $stage_name);
            %table_comp = split_file_to_consistency($link_name_for_ds);
            $table_comp{server} = 'ФАЙЛ';

            #это файл
        }
        elsif ($type eq 'copy') {

            # say 'ZZZ_14_52';

            #это датасет
            $link_name_for_ds = $stage_body->{'output_links'}[0];

           # $link_name_for_ds = get_ds_properties($param_fields, $link_name);
            %table_comp = split_ds_to_consistency($link_name_for_ds);
            $table_comp{server} = 'ДАТАСЕТ';
        }
        elsif ($type eq 'dscapiop') {

#это выгрузка в xml файл, но название странное
# output_file
            $link_name_for_ds =
              get_properties($param_fields, $stage_name, 'output_file');

            # print Dumper $stage_body;
            %table_comp = split_output_file_to_consistency($link_name_for_ds);

            # $link_name_for_ds = $stage_body->{'output_links'}[0];

            # say 'dscapiop $link_name_for_ds: ' . $link_name_for_ds;
            $table_comp{server} = 'XML ФАЙЛ';
        }

        # say "\$link_name_for_ds:$link_name_for_ds";

        $table_comp{schema} = "#$table_comp{schema}#";
    }
    return %table_comp;
}

sub get_properties {
    my ($param_fields, $stage_name, $property_name) = @_;
    my $OLEType = 'CCustomStage';
    my $rec = get_body_of_records($param_fields, $stage_name, $OLEType);
    my $xml;
    for my $rec (@{$rec->{subrecord_body}}) {
        if ($rec->{Name} eq $property_name) {
            $xml = $rec->{Value};
        }
    }
    return $xml;
}

sub split_file_to_consistency {
    my $link_name_for_ds = shift;
    $link_name_for_ds =~ m/
                  \[\&
         (?<quote>
         (:?[\"']|\\")
         )
         (?<schema>
         [\w.]+
         )
         \g{quote}         
         \]
        (?<table_name>
         \w+[.]txt
         )         
         /x;
    my %table_comp = %+;
    return %table_comp;
}

sub split_ds_to_consistency {
    my $link_name_for_ds = shift;
    $link_name_for_ds =~ m/
                  \[\&
         (?<quote>
         (:?[\"']|\\")
         )
         (?<schema>
         [\w.]+
         )
         \g{quote}         
         \]
        (?<table_name>
         \w+[.]ds
         )         
         /x;
    my %table_comp = %+;
    return %table_comp;
}

sub split_output_file_to_consistency {
    my $link_name_for_ds = shift;
    $link_name_for_ds =~ m/
    \#(?<schema>.*?)\#
    (?<table_name>.*)                   
         /x;
    my %table_comp = %+;
    return %table_comp;
}

sub get_xml_properties {
    my ($param_fields, $stage_name) = @_;
    my $OLEType = 'CCustomStage';
    my $rec = get_body_of_records($param_fields, $stage_name, $OLEType);
    my $xml;
    for my $rec (@{$rec->{subrecord_body}}) {
        if ($rec->{Name} eq 'XMLProperties') {
            $xml = $rec->{Value};
        }
    }
    return $xml;
}

sub get_sql_fields {
    my ($param_fields, $link_name) = @_;

    my $OLEType = 'CTrxOutput';    # qw/CTrxOutput CCustomOutput/;
    my $link_body =
      get_parsed_fields_from_all($param_fields, $link_name, $OLEType);
    my $sql_fields  = $link_body->{subrecord_body};
    my @sql_records = ();
    for my $rec (@{$link_body->{subrecord_body}}) {
        if (defined $rec->{SqlType}) {
            push @sql_records, $rec;
        }
    }
    return \@sql_records;
}

sub pexcel_head {
    my ($j, $col, $param_fields, $name, $curr_job) = @_;

#my $sheet=get_worksheet_by_name($param_fields->{workbook},'Revision_History');
    $curr_job->write($j, $col, $name,
        $param_fields->{ref_formats}->{heading});
}

sub pexcel_row {
    my ($j, $col, $param_fields, $name, $curr_job) = @_;
    $curr_job->write($j, $col, $name,
        $param_fields->{ref_formats}->{rows_fmt});
}

sub pexcel_table_links {
    my ($j, $col, $param_fields, $stage, $suffix, $curr_job) = @_;
    pexcel_head($j, $col, $param_fields, $suffix, $curr_job);
    my $q = 0;
    for my $single_field (@{$stage->{$suffix}}) {
        pexcel_row($j + 1, $col + $q, $param_fields, $single_field,
            $curr_job);
        $q++;
    }
    $j = $j + 1;

    # $j = show_stage_prop(
    my $max = show_stage_prop(
        $j, $col, $param_fields, $stage->{$suffix},
        $param_fields->{job_pop}->{only_links}->{stages_with_types},
        '_' . $suffix, $curr_job
    );
    $max = max($max, $j);

    # return $j;
    return $max;
}

sub get_parsed_fields_from_all {
    my ($param_field, $link_name, $OLEType) = @_;
    my %deb_vars = ();
    @deb_vars{'param_field', 'link_name'} = ($param_field, $link_name);

    my $char         = ':';
    my $in_link_name = $link_name;
    my $in_real_link_name =
      substr($in_link_name, index($in_link_name, ':') + 1);

    # say 'in_real_link_name 14:42: ' . $in_real_link_name;
    # say '$OLEType 14:42: ' . $OLEType;

    # debug( 1, $param_field);
    #my $OLEType = 'CTrxOutput';
    my $fields =
      get_body_of_records($param_field, $in_real_link_name, $OLEType);

    # debug( 1, $fields);
    return $fields;
}

sub pexcel_all {
    my ($j, $col, $param_fields, $name, $format_name, $curr_job) = @_;
    $curr_job->write($j, $col, $name,
        $param_fields->{ref_formats}->{$format_name});
}

sub get_caption_fields {
    my $caption_fields =
      'Идентификатор атрибута (таблица.атрибут);A2;fm_grey
 ;A1;fm_grey
Вхождение в проект;B1;fm_purple
Project;B2;fm_purple
Job;C2;fm_purple
 ;C1;fm_purple
ПРИЕМНИК ДАННЫХ;D1;target_field_fmt
Витрины: BCE 13.4, Magnitude, КРЕМ2, Armoni, СИ: БИС, АБС, ЯДРО;D2;target_field_fmt
Схема;E2;target_field_fmt
 ;E1;target_field_fmt
Таблица;F2;target_field_fmt
 ;F1;target_field_fmt
Поле;G2;target_field_fmt
 ;G1;target_field_fmt
Тип данных/Длина;H2;target_field_fmt
 ;H1;target_field_fmt
Вхождение в ключ;I2;target_field_fmt
 ;I1;target_field_fmt
Обязательность;J2;target_field_fmt
 ;J1;target_field_fmt
ИСТОЧНИК ДАННЫХ;K1;target_field_fmt
Витрины : BCE 13.4, Magnitude, КРЕМ2, Armoni (MART_NEW), СИ: БИС, АБС, ЯДРО (DWH);K2;target_field_fmt
Схема;L2;source_field_fmt
 ;L1;source_field_fmt
Таблица;M2;source_field_fmt
 ;M1;source_field_fmt
Поле;N2;source_field_fmt
 ;N1;source_field_fmt
Тип данных/Длина;O2;source_field_fmt
 ;O1;source_field_fmt
Вхождение в ключ;P2;source_field_fmt
 ;P1;source_field_fmt
Алгоритм запроса на источник;Q2;fm_grey
Алгоритм ETL;R2;fm_grey
Правило связей между полями в источнике (внутренние);S2;fm_grey
Правило связи между полями в различных источниках (внешние);T2;fm_grey
Armoni;U2;fm_grey
BCE Retail;V2;fm_light_blue
BCE NonR;W2;fm_light_blue
Magnitude;X2;fm_light_blue
Krem2;Y2;fm_light_blue
Отчет/выгрузка;Z2;fm_light_blue
ID;AA2;fm_green
PARENT_ID;AB2;fm_green
MART:BCE 13.4, Magnitude, КРЕМ2, Armoni;AC2;fm_green
Комментарий;AD2;fm_green;
Дата изменения;AE2;fm_green';

    return $caption_fields;
}

sub get_header_values {
    my $caption_fields = get_caption_fields();
    my @values;
    my @source_fields = split(/\n/, $caption_fields);

    # &enc_terminal();
    # say '22:18';
    for my $curr_field (@source_fields) {
        my %fields = ();
        my @collection = split(/;/, $curr_field);

        # say "$collection[1]";
        #dd (\@collection);
        $fields{'caption'} = $collection[0];
        $fields{'coord'}   = $collection[1];
        $fields{'format'}  = $collection[2];
        push @values, \%fields;
    }

    return \@values;
}

sub fill_rev_history {
    my ($param_fields) = @_;

    # my ($ref_formats, $workbook, $job_prop) = (
    # $param_fields->{ref_formats},
    # $param_fields->{workbook},
    # $param_fields->{job_prop}
    # );
    my $revision_history =
      get_worksheet_by_name($param_fields->{workbook}, 'Revision_History');

    # my $sheet_name = 'Revision_History';

    my $rich_records = $param_fields->{job_prop}->{rich_records};

    # print Dumper $job_prop->{rich_records};
    # 'header_and_job', 'header_fields', 'rich_records',
    # 'parsed_dsx',     'links',         'direction',
    # 'lines'
    # foreach my $worksheet ($workbook->sheets()) {
    # if ($worksheet->get_name() eq $sheet_name) {
    # $revision_history = $worksheet;
    # }
    # }

    my $job_name = get_orchestrate_code($rich_records, 'Name');

    #print Dumper $workbook;
    # my $revision_history = $workbook->sheets(0);
    $revision_history->write(5, 5, 0,
        $param_fields->{ref_formats}->{rows_fmt});
    $revision_history->write(5, 6, 0,
        $param_fields->{ref_formats}->{rows_fmt});
    $revision_history->write_url(
        5, 7,
        'internal:' . substr($job_name, -20) . '_mapping' . '!A2',
        $param_fields->{ref_formats}->{url_format}, $job_name
    );
    $revision_history->write(
        5, 8,
        $param_fields->{job_prop}->{JobDesc},
        $param_fields->{ref_formats}->{rows_fmt}
    );
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
    for my $width (@{$worksheet->{__col_widths}}) {
        $worksheet->set_column($col, $col, $width) if $width;
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
    if (not defined $old_width or $string_width > $old_width) {

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
    my $rows_fmt = $workbook->add_format(align => 'left', border => 1);

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
    $workbook->set_custom_color(40, 141, 180, 226);
    my $map_fmt = $workbook->add_format(
        bold     => 1,
        border   => 2,
        bg_color => 40,
    );
    my $acca_color = $workbook->set_custom_color(40, 230, 230, 230)
      ;    #light grey used in ACCA template

# $workbook->set_custom_color(40, 230,  230,  230); # light grey used in ACCA template
    my $light_orange = $workbook->set_custom_color(43, 255, 226, 171);
    my $ligth_yellow = $workbook->set_custom_color(42, 255, 255, 153);
    my $light_purple = $workbook->set_custom_color(41, 225, 204, 255);
    my $light_green  = $workbook->set_custom_color(44, 204, 255, 153);
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
    my $grey_color = $workbook->set_custom_color(45, 128, 128, 128);
    my $fm_grey =
      add_fmt_with_color($workbook, $target_field_fmt, $grey_color);
    my $purple_color = $workbook->set_custom_color(46, 204, 192, 218);
    $formats{fm_purple} =
      add_fmt_with_color($workbook, $target_field_fmt, $purple_color);

    my $light_blue_color = $workbook->set_custom_color(47, 183, 222, 222);
    $formats{fm_light_blue} =
      add_fmt_with_color($workbook, $target_field_fmt, $light_blue_color);

    my $green_color = $workbook->set_custom_color(48, 0, 176, 80);
    $formats{fm_green} =
      add_fmt_with_color($workbook, $target_field_fmt, $green_color);

    $formats{fm_green_empty} =
      add_fmt_with_color_fake($workbook, $target_field_fmt, $grey_color);

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

sub add_fmt_with_color_fake {
    my ($workbook, $target_field_fmt, $color) = @_;
    my $fm = $workbook->add_format();
    $fm->copy($target_field_fmt);
    $fm->set_bg_color($color);
    $fm->set_border(0);
    return $fm;
}

sub add_fmt_with_color {
    my ($workbook, $target_field_fmt, $color) = @_;
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
    $sheet->add_write_handler(qr[\w], \&store_string_widths);
}

#
# New subroutine "fill_excel_header" extracted - Wed Nov 5 09:54:20 2014.
#
sub fill_excel_header {
    my $ref_formats      = shift;
    my $revision_history = shift;
    my $head_prop        = shift;
    my $date             = strftime "%d.%m.%Y", localtime;
    $revision_history->write(0, 0, "Date",        $ref_formats->{heading});
    $revision_history->write(0, 1, "Version",     $ref_formats->{heading});
    $revision_history->write(0, 2, "Description", $ref_formats->{heading});
    $revision_history->write(0, 3, "Author",      $ref_formats->{heading});
    $revision_history->write(1, 0, $date,         $ref_formats->{date_fmt});
    $revision_history->write(1, 1, "1.0",         $ref_formats->{num_fmt});
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
    $revision_history->write(0, 5, "Project", $ref_formats->{heading});
    $revision_history->write(0, 6, "Server",  $ref_formats->{heading});
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
    $revision_history->write(4, 5, "Id",          $ref_formats->{heading});
    $revision_history->write(4, 6, "Parent_id",   $ref_formats->{heading});
    $revision_history->write(4, 7, "Sequence",    $ref_formats->{heading});
    $revision_history->write(4, 8, "Description", $ref_formats->{heading});
}

sub enc_terminal {
    if (-t) {
        binmode(STDIN,  ":encoding(console_in)");
        binmode(STDOUT, ":encoding(console_out)");
        binmode(STDERR, ":encoding(console_out)");
    }
}

#
# New subroutine "get_next_stage_for_link" extracted - Thu Nov 21 10:27:27 2014.
#
sub get_next_stage_for_link {
    my ($links, $stage, $direction) = @_;

    # input_links output_links
    # @{$stage->{$suffix}}
    my ($out_suffix, $in_suffix) = ('', '');
    if ($direction eq 'start') {
        $out_suffix = 'output_links';
        $in_suffix  = 'input_links';
    }
    elsif ($direction eq 'end') {
        $out_suffix = 'input_links';
        $in_suffix  = 'output_links';
    }

#массив стадий, которые идут сразу за нашей
    my @next_stages = ();

#Выводим все выходные линки из текущей стадии
    for my $out_link_name (@{$stage->{$out_suffix}}) {

        #идем по всем стадиям
        for my $loc_stage (@{$links}) {

#ищем входные линки совпадающие с нашим выходным
            for my $in_link_name (@{$loc_stage->{$in_suffix}}) {
                if ($out_link_name eq $in_link_name) {

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
    my ($cnt_links, $stage, $links_type) = @_;

#также, если стейдж типа ds или это источник в виде базы данных 'pxbridge'
#у которого нет входящих линков для 1-го и выходящих для последнего
#точки приземления! (Андрей Бабуров)

    my $is_dataset = 'no';

    #кладем
    if ($cnt_links == 1
        && substr(${$stage->{$links_type}}[0], -2) eq 'ds')
    {
        $is_dataset = 'yes';
    }
    return $is_dataset;
}

sub check_for_started {
    my ($cnt_links, $stage, $ref_start_stages_of, $is_dataset) = @_;
    return (
        (   exists $ref_start_stages_of->{$stage->{operator_name}}
              && $cnt_links == 0
        )
          || ($is_dataset eq 'yes')
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
    foreach my $stage (@{$parsed_dsx}) {
        my %only_stages  = ();
        my @input_links  = ();
        my @output_links = ();
        $only_stages{stage_name}    = $stage->{stage_name};
        $only_stages{operator_name} = $stage->{operator_name};
        if ($stage->{ins}->{in} eq 'yes') {
            for my $inputs (@{$stage->{ins}->{inputs}}) {
                my %in_links = ();
                $in_links{link_name} = $inputs->{link_name};

                $in_links{is_param}      = 'no';
                $in_links{trans_name}    = $inputs->{trans_name};
                $in_links{operator_name} = $stage->{operator_name};
                $in_links{stage_name}    = $stage->{stage_name};
                $in_links{inout_type}    = $inputs->{inout_type};

                if ($inputs->{is_param} eq 'yes') {
                    $in_links{is_param}         = 'yes';
                    $in_links{params}           = $inputs->{params};
                    $in_links{link_keep_fields} = $inputs->{link_keep_fields};

                    my $in_link_name = $inputs->{link_name};
                    my $in_real_link_name =
                      substr($in_link_name, index($in_link_name, $char) + 1);

                }
                push @only_links,  \%in_links;
                push @input_links, $inputs->{link_name};
                $stages_with_types{$inputs->{link_name} . '_'
                      . $inputs->{inout_type}} = \%in_links;
            }
        }
        $only_stages{input_links} = \@input_links;
        if ($stage->{ins}->{out} eq 'yes') {
            for my $outputs (@{$stage->{ins}->{outputs}}) {
                my %out_links = ();
                $out_links{link_name} = $outputs->{link_name};

                $out_links{is_param}      = 'no';
                $out_links{trans_name}    = $outputs->{trans_name};
                $out_links{operator_name} = $stage->{operator_name};
                $out_links{stage_name}    = $stage->{stage_name};
                $out_links{inout_type}    = $outputs->{inout_type};

                if ($outputs->{is_param} eq 'yes') {
                    $out_links{is_param} = 'yes';
                    $out_links{params}   = $outputs->{params};
                    $out_links{link_keep_fields} =
                      $outputs->{link_keep_fields};

                    my $out_link_name = $outputs->{link_name};
                    my $out_real_link_name =
                      substr($out_link_name,
                        index($out_link_name, $char) + 1);

                }
                push @only_links,   \%out_links;
                push @output_links, $outputs->{link_name};
                $stages_with_types{$outputs->{link_name} . '_'
                      . $outputs->{inout_type}} = \%out_links;
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
        $cnt_links{$_->{link_name} . '_' . $_->{inout_type}}++;
    }

    # print DumpTree(\%out_hash, '\%out_hash');
    return \@only_stages_and_links;
}

#
# New subroutine "fill_excel_stages_and_links" extracted - Wed Nov 5 16:12:45 2014.
#

sub fill_way_and_links {
    my ($links, $direction) = @_;

  # my $links        = $all->{job_pop}->{only_links}->{only_stages_and_links};
    my @start_stages    = qw/copy pxbridge import export dscapiop/;
    my %start_stages_of = map { $_ => 1 } @start_stages;
    my $max             = 0;
    my $links_type = ($direction eq 'start') ? 'input_links' : 'output_links';
    my %start_stages_name = ();
    my %a_few_stages      = ();
    my $cnt_stages        = 0 + @{$links};

    #    say "number of links: $cnt_stages";
    #хэш стейджей с объектами
    # debug(1, $links);

    my %stages_body;
    for my $stage (@{$links}) {
        $stages_body{$stage->{stage_name}} = $stage;
        my $cnt_links = 0 + @{$stage->{$links_type}};

        my $is_dataset = check_for_dataset($cnt_links, $stage, $links_type);
        my $is_started_links =
          check_for_started($cnt_links, $stage, \%start_stages_of,
            $is_dataset);

        if ($is_started_links) {

       #находим все начальные линки,их имена!!!
            $a_few_stages{$stage->{stage_name}}++;
        }
        my %link_collection = ();
        for my $direction ('start', 'end') {
            my $assoc_stages =
              get_next_stage_for_link($links, $stage, $direction);
            $link_collection{$direction} = $assoc_stages;
        }
        $start_stages_name{$stage->{stage_name}} = \%link_collection;
    }

    # debug(1,\%start_stages_name);
    my ($lines) =
      calculate_right_way_for_stages($direction, $links, \%a_few_stages,
        \%start_stages_name);

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
    foreach my $few_stage (sort keys %{$ref_a_few_stages}) {
        $lines{$few_stage}++;
        my @elements   = ();
        my @levels     = ();
        my %in_already = ();
        for (my $i = 0; $i < $cnt_ctages; $i++) {
            my %stages_in_level    = ();
            my %collect_stages     = ();
            my $ref_collect_stages = \%collect_stages;

            #print "$i\n";
            if ($i == 0) {
                $collect_stages{$few_stage} = 1;
                $in_already{$few_stage}++;
                push @levels, \%collect_stages;

       #say "Первый элемент: @{[ sort keys %collect_stages ]}\n";
                my $ref_0_stages =
                  get_next_stage_in_hash($few_stage, $ref_start_stages_name,
                    $direction);
                push @levels, $ref_0_stages;
                foreach my $stg (keys %{$ref_0_stages}) {
                    $in_already{$stg}++;
                }

      #say "Второй элемент: @{[ sort keys %{$ref_0_stages} ]}\n";
            }
            elsif ($i > 1) {
                my $prev_stages = $levels[$i - 1];
                foreach my $prev_stage (sort keys %{$prev_stages}) {
                    my $ref_stages = get_next_stage_in_hash($prev_stage,
                        $ref_start_stages_name, $direction);
                    $ref_collect_stages =
                      merge($ref_collect_stages, $ref_stages);   #$ref_stages;

                }
                my %hash_for_check = %{$ref_collect_stages};

#проверяем получившийся хэш на стейджи, которые уже были
                foreach my $stg2 (keys %hash_for_check) {
                    if (defined $in_already{$stg2}) {
                        delete $hash_for_check{$stg2};
                    }

                }

                $ref_collect_stages = \%hash_for_check;
                if (!keys %{$ref_collect_stages}) {
                    last;
                }
                push @levels, $ref_collect_stages;    #\%collect_stages;
                foreach my $stg3 (keys %{$ref_collect_stages}) {
                    $in_already{$stg3}++;
                }

#               say "Третий элемент: @{[ sort keys %{$ref_collect_stages} ]}\n";
            }
        }
        $lines{$few_stage} = \@levels;
    }

  # print DumpTree( \%lines, '$hash_ref_lines and direction: ' . $direction );
  # print DumpTree( $links, '$links ' );
    return (\%lines);
}

sub get_next_stage_in_hash {
    my ($prev_stage, $ref_start_stages_name, $direction) = @_;

#enc_terminal();
#say 'Для начала выясним, что у нас за переменные:';
#say 'Будем считать, что в хэше несколько стейджей,тогда пройдем по ним всем!!!:';
#say 'Предыдущий стейдж :' . $prev_stage;
    my $ref_link_array = $ref_start_stages_name->{$prev_stage}->{$direction};
    my %stage_collections = ();
    for my $link (@{$ref_link_array}) {

        #       say $link->{stage_name};
        $stage_collections{$link->{stage_name}}++;
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
    while ($body_for_fields =~ m/$field/g) {
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
    while ($body =~ m/$link/g) {
        my %link_param = ();

        # _Part_Sort         _Sort_Part
        # $link_param{link_name} =~ s/(_Part_Sort|_Sort_Part)//g;
        my $link = $+{link_name};

#избавляемся от промежуточного стейджа сортировки,
#пока для наших задач он не нужен и его можно игнорировать
        $link =~ s/(_Part_Sort|_Sort_Part)//g;
        $link_param{link_name} = $link;

        # say 'in: ' . $link_param{link_name};
        $link_param{link_type}  = $+{link_fields};
        $link_param{inout_type} = 'input_links';
        $link_param{trans_name} = $+{trans_name}
          if defined $+{trans_name};
        $link_param{is_param} = 'no';

        if (defined $+{link_fields}) {
            $link_param{is_param} = 'yes';
            $link_param{params}   = parse_fields($+{link_fields});
            $link_param{link_keep_fields} =
              parse_keep_fields($+{link_keep_fields})
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
(?<link_name>
.*?
\w+.ds
)
)'
}xs;

    #\[.*?\]
    while ($body =~ m/$link/g) {
        my %link_param = ();
        $link_param{link_name} = $+{link_name};

        # say 'out: ' . $link_param{link_name};
        $link_param{link_type}  = $+{link_fields};
        $link_param{inout_type} = 'output_links';

        #$link_param{link_type} = $+{link_type};
        $link_param{trans_name} = $+{trans_name}
          if defined $+{trans_name};
        $link_param{is_param} = 'no';
        if (defined $+{link_fields})

          #if ( length( $link_param{link_type} ) >= 6
          #&& substr( $link_param{link_type}, 0, 6 ) eq 'modify' )
        {
            $link_param{is_param} = 'yes';
            $link_param{params}   = parse_fields($+{link_fields});
            $link_param{link_keep_fields} =
              parse_keep_fields($+{link_keep_fields})
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

    my ($inputs, $outputs) = ('', '');
    $outs{in}               = 'no';
    $outs{out}              = 'no';
    $outs{body}             = $stage_body;
    $outs{operator_options} = parse_operator_options($stage_body);
    if ($stage_body =~ $inputs_rx) {
        $outs{inputs} = parse_in_links($+{inputs_body});
        $outs{in}     = 'yes';
    }
    if ($stage_body =~ $outputs_rx) {
        $outs{outputs} = parse_out_links($+{outputs_body});
        $outs{out}     = 'yes';
    }
    return \%outs;
}

sub parse_operator_options {
    my $stage = shift;

    $stage =~ m{
          (?<stage_body>
\#\#\#\#[ ]STAGE:[ ](?<stage_name>[\w.]+)[\n]
\#\#[ ]Operator[\n]
(?<operator_name>\w+)[\n]
\#\#[ ]Operator[ ]options
(?<operator_options>
.*?
\n
)
\#\#
.*?
[\n]
;
)
        }xsm;

    my $operator_options = $+{operator_options};
    my %field_param      = ();
    if (defined $operator_options) {
        my $field = qr{
            (:?
(?<param_name>
-\w+
)(:?[ ]
(?<param_value>.*?)
|
)
)
(?=
\n-|\n\n
)
}xs;

        while ($operator_options =~ m/$field/g) {
            $field_param{$+{param_name}} = $+{param_value};
        }
    }
    return \%field_param;

}

sub make_orchestrate_regexp {

    my $ORCHESTRATE_BODY_RX = qr{
(?<stage_body>
\#\#\#\#[ ]STAGE:[ ](?<stage_name>[\w.]+)[\n]
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
    while ($data =~ m/$ORCHESTRATE_BODY_RX/xsg) {
        my %stage = ();
        my $ins   = parse_stage_body($+{stage_body});
        $stage{ins}           = $ins;
        $stage{stage_name}    = $+{stage_name};
        $stage{operator_name} = $+{operator_name};
        push @parsed_dsx, \%stage;
    }
    return \@parsed_dsx;
}

sub get_body_of_records {
    my ($param_fields, $search_name, $OLEType) = @_;

    my $rich_records = $param_fields->{job_prop}->{rich_records};
    my $rec;

    my $curr_ref_array;

    my @OLEType = qw/CTrxOutput CCustomOutput CCustomStage $OLEType/;

    for my $rec1 (@{$rich_records}) {
        my $loc_name = $rec1->{'fields'}->{'Name'};
        my $loc_type = $rec1->{'fields'}->{'OLEType'};

        if ($loc_name eq $search_name && any { $loc_type eq $_ } @OLEType) {
            $curr_ref_array = $rec1;
        }
    }
    return $curr_ref_array;
}

sub get_orchestrate_code {
    my ($rich_records, $seach_node) = @_;    #shift;
    my $rec;
    my $Identifier = 'ROOT';

    # my $seach_node = 'OrchestrateCode';

    my $curr_ref_array;

    for my $rec1 (@{$rich_records}) {
        my $loc_identifier = $rec1->{'fields'}->{'Identifier'};
        if (defined $loc_identifier && $loc_identifier eq $Identifier) {
            $curr_ref_array = $rec1;
        }
    }
    my $orch_code = $curr_ref_array->{'fields'}->{$seach_node};
    return $orch_code;
}

sub enrich_records {
    my $ref_array_dsrecords = shift;
    my @richer_record       = ();
    for my $rec (@{$ref_array_dsrecords}) {
        my $fields = get_identifier_and_field_of_record($rec);
        push @richer_record, pack_fields($fields);
    }
    return \@richer_record;
}

sub pack_fields {
    my $fields      = shift;
    my %new_fields  = ();
    my $identtifier = '';
    if (defined $fields->{identifier}) {
        $new_fields{identifier} = $fields->{identifier};
        $new_fields{fields}     = split_fields_by_new_line(
            $fields->{record_fields_body1} . $fields->{record_fields_body2});
        $new_fields{subrecord_body} =
          reformat_subrecord($fields->{subrecord_body});
    }
    elsif (defined $fields->{identifier2}) {
        $new_fields{identifier} = $fields->{identifier2};
        $new_fields{fields} =
          split_fields_by_new_line($fields->{record_fields_body});
    }
    return \%new_fields;
}

sub get_identifier_and_field_of_record {
    my $data   = shift;
    my %fields = ();
    if ($data =~ /
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
    return (\%fields);

}

sub reformat_subrecord {
    my $curr_record      = shift;
    my $ref_dssubrecords = split_by_subrecords($curr_record);
    my @subrecords       = ();
    for my $subrec (@{$ref_dssubrecords}) {
        push @subrecords, split_fields_by_new_line($subrec);
    }
    return \@subrecords;
}

sub split_by_subrecords {
    my $curr_record = shift;
    local $/ = '';    # Paragraph mode
    my @dssubrecords = ($curr_record
          =~ / BEGIN[ ]DSSUBRECORD([\n]   .*?  )END[ ]DSSUBRECORD /xsg);
    return \@dssubrecords;
}

sub get_name_and_body {
    my $data = shift;
    $data =~ /
BEGIN[ ]DSJOB
.*?
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
        my ($value, $name) = ('', '');
        if (defined $+{name}) {
            $name  = $+{name};
            $value = $+{value};
        }
        elsif (defined $+{name2}) {
            $name  = $+{name2};
            $value = $+{value2};
        }
        $fields_and_values{$name} = $value;
    }
    return \%fields_and_values;
}

sub clear_from_back_slash {
    my $string = shift;
    if (defined $string) {
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
      ($data =~ / ( BEGIN[ ]DSRECORD[\n]   .*?  END[ ]DSRECORD ) /xsg);
    return \@records;
}

sub debug {
    my ($run_as_a_one, $value) = @_;
    state $i= 1;
    if (($i == 1) || ($run_as_a_one != 1)) {
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

    write_file_utf8('dump.html', $dump);

#-------------------------------------------------------------------------------

}

sub write_file_utf8 {

    my $name   = shift;
    my $string = shift;
    my $ustr   = $string;    #"simple unicode string \x{0434} indeed";

    {
        open(my $FH, ">:encoding(UTF-8)", $name)
          or die "Failed to open file - $!";

        write_file($FH, $ustr)
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
