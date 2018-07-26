#!/usr/bin/env perl
use strict;
use File::Slurp;
use File::Basename;
use File::Path;

my $file_type_list = ['All', 'Plasma', 'MAF'];

my $outdir = '/tmp/' . File::Basename::basename($0) . '/' . time();

my $outfile = $outdir . '/batch_converter.sh';

my $infile = $ARGV[0];
if (!defined($infile)){
    die "Usage : perl $0 directory-list-file\n";
}

my $seq_id_to_dir_lookup = {};

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


my $seq_id_to_assets_lookup = {};

for my $seq_id (sort keys %{$seq_id_to_dir_lookup}){

    my $lookup = {};
    
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
}


&write_batch_file();

sub write_batch_file {
    
    open (OUTFILE, ">$outfile") || die "Could not open file '$outfile' in write modee :$!";
    
    for my $seq_id (sort keys %{$seq_id_to_assets_lookup}){

	print OUTFILE "\n\n$seq_id\n";
	
	for my $file_type (sort @{$file_type_list}){
	    
	    my $file = $seq_id_to_assets_lookup->{$seq_id}->{$file_type};
	    
	    my $cmd = "python converter.py $file $seq_id --outdir $outdir/$seq_id";
	    
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
