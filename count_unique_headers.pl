#!/usr/bin/env perl
use strict;
use File::Slurp;
my $infile = $ARGV[0];
if (!defined($infile)){
    die "Usage : perl $0 infile\n";
}

open (INFILE, "<$infile") || die "Could not open file '$infile' in read mode : $!";

my $lookup = {};

while (my $line = <INFILE>){
    chomp $line;
    my @parts = split("\t", $line);
    my $i = 0;
    for my $part (@parts){
	$i++;
	push(@{$lookup->{$part}}, $i);
    }
    last;
}

for my $header (sort keys  %{$lookup}){
    my $list = $lookup->{$header};
    if (scalar(@{$list}) > 1){
	print "$header occured at column(s):\n";
	print join(', ', @{$list}) . "\n";
    }
}
  
