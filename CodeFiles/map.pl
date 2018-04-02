#!/usr/bin/env perl -wl

use strict;
use warnings;
use Data::Dumper;

my %reduce;

while(<STDIN>) {
  chomp;
  my ($city, $product, $cost) = split(',');
  print "$city $cost";
}
