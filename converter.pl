#!/usr/bin/env perl
use strict;
use Carp;
use Term::ANSIColor;
use Pod::Usage;
use File::Path;
use File::Basename;
use File::Copy;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev);


use constant TRUE => 1;

use constant FALSE => 0;

use constant DEFAULT_CREATE_SYMLINK => FALSE;

use constant DEFAULT_HEADER_ROW_COUNT => 1;

use constant DEFAULT_VERBOSE => TRUE;

use constant DEFAULT_OUTDIR => '/tmp/' . File::Basename::basename($0) . '/' . time();


## Will make this configurable
my $changestovcf_exec = './changestovcf.pl';

## Will make this configurable
## Can make this file type specific
##
my $COLUMNS = ['ChangeUID', 'IndexString', 'Coverage', 'MinQualityScore', 'MaxQualityScore', 'AverageQualityScore',
	       'DistinctCoverage', 'MutPct', 'Chrom', 'Start', 'End', 'BaseFrom', 'BaseTo', 'LookupKey', 'ENSG',
	       'GeneName'];

my $QUALIFIED_COLUMNS_LOOKUP = {};

## Will make this configurable
## The number of rows that are considered the header portion
## of the file and should be stripped prior to invoking the
## changestovcf executable.
my $HEADER_ROW_COUNT = 1;

my (
    $help,
    $man,
    $infile,
    $sample_id,
    $verbose,
    $create_symlink,
    $outdir,
    $header_row_count
    );

my $results = GetOptions (
    'infile=s'           => \$infile,
    'sample_id=s'        => \$sample_id,
    'verbose=s'          => \$verbose,
    'create_symlink=s'   => \$create_symlink,
    'help|h'             => \$help,
    'man|m'              => \$man,
    'outdir=s'           => \$outdir,
    'header_row_count=s' => \$header_row_count,
    );

&checkCommandLineArguments();

&main();


##------------------------------------------------------
##
##  END OF MAIN -- SUBROUTINES FOLLOW
##
##------------------------------------------------------

sub checkCommandLineArguments {

    if ($man){
    	&pod2usage({-exitval => 1, -verbose => 2, -output => \*STDOUT});
    }

    if ($help){
    	&pod2usage({-exitval => 1, -verbose => 1, -output => \*STDOUT});
    }

    my $fatalCtr=0;

    if (!defined($infile)){

        printBoldRed("--infile was not specified");

    	$fatalCtr++;
    }

    if (!defined($sample_id)){

        printBoldRed("--sample_id was not specified");

    	$fatalCtr++;
    }


    if (!defined($verbose)){

        $verbose = DEFAULT_VERBOSE;

        printYellow("--verbose was not specified and therefore was set to default '$verbose'");
    }

    if (!defined($header_row_count)){

        $header_row_count = DEFAULT_HEADER_ROW_COUNT;

        printYellow("--header_row_count was not specified and therefore was set to default '$header_row_count'");
    }

    if (!defined($create_symlink)){

        $create_symlink = DEFAULT_CREATE_SYMLINK;

        printYellow("--create_symlink was not specified and therefore was set to default '$create_symlink'");
    }

    if (!defined($outdir)){

    	$outdir = DEFAULT_OUTDIR;

        printYellow("--outdir was not specified and therefore was set to default '$outdir'");
    }


    $outdir = File::Spec->rel2abs($outdir);

    if (!-e $outdir){

        mkpath ($outdir) || die "Could not create output directory '$outdir' : $!";

        printYellow("Created output directory '$outdir'");

    }

    if ($fatalCtr> 0 ){

    	die "Required command-line arguments were not specified\n";
    }
}

sub printBoldRed {

    my ($msg) = @_;
    print color 'bold red';
    print $msg . "\n";
    print color 'reset';
}

sub printYellow {

    my ($msg) = @_;
    print color 'yellow';
    print $msg . "\n";
    print color 'reset';
}

sub printGreen {

    my ($msg) = @_;
    print color 'green';
    print $msg . "\n";
    print color 'reset';
}

sub main {

    if (!-e $infile){
    	die "infile '$infile' does not exist";
    }

    if ($verbose){
        print("The input file is '$infile'\n");
    	print("The sample_id is '$sample_id'\n");
    }

    my $infile_basename = File::Basename::basename($infile);

    my $file_type = 'Unknown';

    if ($infile_basename =~ m/allchanges\.txt$/i){

        $file_type = 'Unfiltered';
    }
    elsif ($infile_basename =~ m/plasmachanges\.txt$/i){

        $file_type = 'Filtered';
    }
    elsif ($infile_basename =~ m/vv_changes\.txt/i){

        $file_type = 'MAF-Filtered';
    }
    else {

        print("Unsupported file type '$infile'\n");
    	exit(0);
    }

    if ($verbose){
        print("Found file type '$file_type'\n");
    }

    if ($infile_basename =~ m/\.gz$/){
        $infile = &uncompress_file($infile);
    }

    if ($verbose){
        print("Including only the following column names : " . join(', ', @{$COLUMNS})) . "\n";
    }


    # Retrieve subset of columns filtered by column
    # names specified in the columns list.

    my $outfile = $outdir . '/' . $sample_id . '_' . $file_type . '.csv';

    &filter_file_without_pandas($infile, $outfile);

    my $line_count = &get_file_line_count($infile);

    my $tail_count = $line_count - 1;

    my $out_vcf_file = $outdir . '/' . $sample_id . "_" . $file_type . '.vcf';

    &run_changestovcf($outfile, $out_vcf_file, $tail_count);

    if ($create_symlink){

        my $symlink_file  = $outdir . '/' . $infile_basename;

        &create_infile_symlink($infile, $symlink_file);
    }
}

sub execute_cmd {

    my ($cmd) = @_;

    if ($verbose){
        print("Will attempt to execute '$cmd'\n");
    }

    eval {
    	qx($cmd);
    };

    if ($?){
    	confess "Encountered some error while attempting to execute '$cmd' : $@ $!";
    }

}

sub uncompress_file {

    my ($infile, $verbose) = @_;

    my $cmd = "gunzip -f " . $infile;

    if ($infile =~ m/\.gz$/){
    	$infile =~ s/\.gz$//;
    }

    &execute_cmd($cmd);

    return $infile;
}

sub get_qualified_columns_lookup {

    my $lookup = {};

    for my $column_name (@{$COLUMNS}){
        $lookup->{$column_name}++;
    }

    return $lookup;
}

sub filter_file_without_pandas {

    my ($infile, $outfile) = @_;

    my $line_ctr = 0;

    my $qualified_column_lookup = &get_qualified_columns_lookup();

    my $qualified_column_number_lookup = {};

    my $qualified_column_number_list = [];

    my $filtered_record_list = [];

    my $qualified_header_list = [];


    open (INFILE, "<$infile") || die "Could not open input file '$infile' in read mode : $!";


    my $already_found_lookup = {};

    my $row_ctr = 0;

    while ( my $line = <INFILE>){

    	chomp $line;

    	my @parts = split("\t", $line);

    	my $row = \@parts;

    	$row_ctr++;

    	if ($row_ctr == 1){

    	    # This is the header so want to derive the column numbers for our
    	    # fields of interest.

    	    my $header_ctr = 0;

    	    for my $header (@{$row}){

        		if (exists $qualified_column_lookup->{$header}){

        		    if (! exists $already_found_lookup->{$header}){

            			$qualified_column_number_lookup->{$header_ctr} = $header;

            			push(@{$qualified_column_number_list}, $header_ctr);

            			push(@{$qualified_header_list}, $header);

            			$already_found_lookup->{$header}++;
        		    }
        		}

        		$header_ctr++;

    	    }
    	}
    	else {

    	    my $record = [];

    	    for my $field_num (@{$qualified_column_number_list}){

        		push(@{$record}, $row->[$field_num]);
    	    }

    	    push(@{$filtered_record_list}, $record);
    	}
    }



    open (OUTFILE, ">$outfile") || die "Could not open output file '$outfile' in write mode : $!";

    print OUTFILE join(',', @{$qualified_header_list}) . "\n";

    for my $row (@{$filtered_record_list}){

    	print OUTFILE join(',', @{$row}) . "\n";
    }

    print("Wrote output file CSV file '$outfile'\n");
}

sub get_file_line_count {

    my ($fname) = @_;

    open (INFILE, "<$infile") || die "Could not open input file '$infile' in read mode : $!";

    my $row_ctr = 0;

    while ( my $line = <INFILE>){

    	chomp $line;
    	$row_ctr++;
    }

    return $row_ctr;
}


sub run_changestovcf {

    my ($infile, $outfile, $count) = @_;

    my $cmd = "tail -" . $count . ' ' . $infile . ' | perl ' . $changestovcf_exec . ' > ' . $outfile;

    &execute_cmd($cmd);

    print("VCF file '$outfile' is ready\n");
}


sub create_infile_symlink {

    my ($infile, $symlink_file) = @_;

    my $cmd = "ln -s $infile $symlink_file";

    &execute_cmd($cmd);
}


