#!/usr/bin/env perl
use strict;
use File::Slurp;
use File::Basename;
use File::Path;

use constant TRUE => 1;
use constant FALSE => 0;

my $write_to_seq_dir = TRUE;

my $PERL_MODE = TRUE;

my $file_type_list = ['All', 'Plasma', 'MAF'];

my $outdir = '/tmp/' . File::Basename::basename($0) . '/' . time();

my $outfile = $outdir . '/batch_converter.sh';

my $infile = $ARGV[0];
if (!defined($infile)){
    die "Usage : perl $0 directory-list-file\n";
}

my $seq_id_to_dir_lookup = {};

&derive_seq_id_list();

my $seq_id_to_assets_lookup = {};

&identify_all_assets();

&write_batch_file();

print File::Spec->rel2abs($0) . " execution completed\n";
exit(0);

##--------------------------------------------------
##
##  END OF MAIN -- SUBROUTINES FOLLOW
##
##--------------------------------------------------

sub derive_seq_id_list {

    my @lines = read_file($infile);

    chomp @lines;

    for my $seq_dir (@lines){

	if ($seq_dir =~ /^\s*$/){
	    next;
	}

	if ($seq_dir =~ /^#/){
	    next;
	}

	if (!-e $seq_dir){
	    warn "$seq_dir does not exist";
	}

	if (!-d $seq_dir){
	    warn "$seq_dir is not a directory";
	}

	my $seq_id = File::Basename::basename($seq_dir);

	if ($seq_id =~ m/^(MAYO\d{4}P_PS_Seq2)_\d{8}_\d{6}$/){
	    $seq_id = $1;
	    print "Found seq_id '$seq_id'\n";
	    $seq_id_to_dir_lookup->{$seq_id} = $seq_dir;
	}
	else {
	    warn "Could not parse '$seq_id'\n";
	}
    }
}

sub identify_all_assets {

    my $seq_id_ctr = 0;

    for my $seq_id (sort keys %{$seq_id_to_dir_lookup}){

	$seq_id_ctr++;

	my $seq_dir = $seq_id_to_dir_lookup->{$seq_id};

	my $seq_outdir = $outdir . '/' . $seq_id;

	mkpath($seq_outdir) || die "Could not create output directory '$seq_outdir' : $!";

	print "Created seq output directory '$seq_outdir'\n";

	## MAF file
	my $maf_file = &get_maf_file($seq_dir, $seq_id);#= $seq_dir . '/' . $seq_id . '*novo/misc/' . $seq_id . '*VV*';

	if (!-e $maf_file){
	    warn "Could not find MAF file '$maf_file'";
	}
	else {
	    $seq_id_to_assets_lookup->{$seq_id}->{'MAF'} = $maf_file;
	}

	## Plasma Changes file
	my $plasma_changes_file = $seq_dir . '/' . File::Basename::basename($seq_dir) . '.PlasmaChanges.txt';
	if (!-e $plasma_changes_file){
	    die "Could not find Plasma Changes file '$plasma_changes_file'";
	}


	$seq_id_to_assets_lookup->{$seq_id}->{'Plasma'} = $plasma_changes_file;


	## All Changes file
	## E.g.: /cifs/pods/data39-pgdx-pod22/Samples/MAYO0123P_PS_Seq2_20180722_171158/MAYO0123P_PS_Seq2_novo/MAYO0123P_PS_Seq2_novo.allchanges.txt
	my $all_changes_file = &get_all_changes_file($seq_dir, $seq_id); #$seq_dir . '/' . $seq_id . '*/' . $seq_id . '*.allchanges.txt';
	if (!-e $all_changes_file){
	    die "Could not find All Changes file '$all_changes_file'";
	}

	$seq_id_to_assets_lookup->{$seq_id}->{'All'} = $all_changes_file;

	if ($seq_id_ctr == 1){
#	    last;
	}
    }
}


sub write_batch_file {

    open (OUTFILE, ">$outfile") || die "Could not open file '$outfile' in write mode :$!";

    for my $seq_id (sort keys %{$seq_id_to_assets_lookup}){

	print OUTFILE "\n\necho 'Processing $seq_id'\n";

	for my $file_type (sort @{$file_type_list}){

	    my $file = $seq_id_to_assets_lookup->{$seq_id}->{$file_type};

	    my $results_dir = $outdir . '/' . $seq_id;

	    if ($write_to_seq_dir){

		my $seq_dir = $seq_id_to_dir_lookup->{$seq_id};

		$results_dir = $seq_dir . '/vcf-data-prep/';
	    }

	    if (!-e $results_dir){
            print OUTFILE "\necho 'Creating directory $results_dir'\n";
    		print OUTFILE "mkdir -p $results_dir\n";
		#mkpath($results_dir) || die "Could not create results directory '$results_dir' : $!";
	    }

	    my $cmd;

        my $stdout = $results_dir . '/' . File::Basename::basename($file) . '.stdout';
        my $stderr = $results_dir . '/' . File::Basename::basename($file) . '.stderr';

	    if ($PERL_MODE){

		$cmd = "perl converter.pl --infile $file --sample_id $seq_id --outdir $results_dir 1>>$stdout 2>>$stderr";
	    }
	    else {

		$cmd = "python converter.py $file $seq_id --outdir $results_dir 1>>$stdout 2>>$stderr";
	    }

        print OUTFILE "\necho 'About to execute $cmd'\n";
	    print OUTFILE $cmd . "\n";
	}
    }

    close OUTFILE;

    print "Wrote batch file '$outfile'\n";
}

sub get_results {

    my ($cmd) = @_;

    print "About to execute '$cmd'\n";

    my @results;

    eval {
	@results = qx($cmd);
    };

    if ($?){
	die "Encountered some error while attempting to execute '$cmd' : $! $@";
    }

    chomp @results;

    return \@results;
}

sub get_maf_file {

    my ($seq_dir, $seq_id) =@_;

    print "Searcing for MAF file in $seq_dir\n";

    my $cmd = "find $seq_dir -name '*VV*'";

    my $results = &get_results($cmd);

    for my $file (@{$results}){

	print "Found candidate '$file'\n";

	if ($file =~ m|/misc/|){
	    return $file;
	}
    }

    die "Did not find MAF file in $seq_dir";
}

sub get_all_changes_file {

    my ($seq_dir, $seq_id) =@_;

    print "Searching for All Changes file in $seq_dir\n";

    my $cmd = "find $seq_dir -name '*.allchanges.txt'";

    my $results = &get_results($cmd);

    for my $file (@{$results}){

	print "Found candidate '$file'\n";

	my $dirname = File::Basename::dirname($file);
	my $dirname2 = File::Basename::dirname($dirname);
	my $basename = File::Basename::basename($dirname);
	if (($basename =~ m/novo/) && ($dirname2 eq $seq_dir)){
	    return $file;
	}
    }

    die "Did not find MAF file in $seq_dir";
}
