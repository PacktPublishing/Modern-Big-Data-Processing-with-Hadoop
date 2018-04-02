#!/usr/bin/env perl -wl

use strict;
use warnings;
use Data::Dumper;

my %reduce;

while(<STDIN>) {
  chomp;
  my ($city, $cost) = split(/\s+/);
  $reduce{$city} = 0 if not defined $reduce{$city};
  $reduce{$city} += $cost;
}

print "-" x 24;
printf("%-10s : %s\n", "City", "Total Cost");
print "-" x 24;
foreach my $city (sort keys %reduce) {
  printf("%-10s : %d\n", $city, $reduce{$city});
}
