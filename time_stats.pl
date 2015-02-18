#!/usr/bin/perl

use XML::LibXML;
use Date::Parse;
use File::Basename;
use Getopt::Std;
use strict;

# usage: perl $0 -f file_with_filename_listings files

my %opts;

getopts('b:', \%opts);

my $batch_name = lc($opts{'b'}) if defined $opts{'b'};

# PARSER
die "Specify batch name!\n" unless $batch_name;
die "bad batch name!\n" unless $batch_name =~ /\d+$/;
my $batch_N = $&;

die "usage: perl $0 -b batch_name files\n" unless @ARGV;

# die "Malformed batch listing name\n" unless $flname =~ /-(\d+)_files/;
# my $batch_N = $1;

my $parser = XML::LibXML->new();
$parser->keep_blanks(0);
my $beg = 100000000000;
my $end = 0;
my %H;
my $doc_N = 0;
my $secstot = 0; # total counting header times
foreach (@ARGV) {
  my $fname = $_;
  my $fh = &open_maybe_bz2($fname);
  my ($dbeg, $dend, @DD) = &stat_doc($fh);
  next unless defined $dbeg;
  $doc_N++;
  $beg = $dbeg if $dbeg < $beg;
  $end = $dend if $dend > $end;
  foreach my $dd (@DD) {
    my $name = $dd->{name};
    my $secs =  $dd->{secs};
    $secstot += $secs;
   $H{$name} += $secs;
  }
}

my $dtot = $end - $beg;
#open (my $fo, ">${batch_N}.org") or die;
foreach my $name (keys %H) {
  my $h = $H{$name};
  printf("| %s | %i | %.2f | %.2f |\n", $name, $H{$name}, 100*$H{$name} / $secstot, 100*$H{$name} / $dtot);
}
print "\n\n";
print "batch:$batch_N\n";
print "docs:$doc_N\n";
print "processing_secs:$secstot\n";
print "elapsed_secs:$dtot\n";
print "beg_tspan:$beg\n";
print "end_tspan:$end\n";

sub stat_doc {

  my ($fh) = @_;

  binmode $fh; # remove utf-ness

  my $doc = $parser->parse_fh($fh);
  my $doc_elem = $doc->getDocumentElement;

  my @D;
  foreach my $elem ($doc_elem->findnodes('/NAF/nafHeader/linguisticProcessors')) {
    push @D, &lingProc_lps($elem);
  }
  return undef unless @D;
  if (@D == 1) {
    return ($D[0]->{tstamp}, $D[0]->{tstamp}, @D);
  }
  @D = sort { $a->{tstamp} <=> $b->{tstamp} } @D;
  my $prev = 0;
  foreach my $r (@D) {
    if ($prev) {
      $r->{secs} = ($r->{tstamp} - $prev);
    }
    $prev = $r->{tstamp};
    #print "\n";
  }
  return ($D[0]->{tstamp}, $D[-1]->{tstamp}, @D);
}

sub lingProc_lps {
  my $lingproc_elem = shift;
  my @D;
  my $pre = $lingproc_elem->getAttribute("layer")."#";
  foreach my $elem ($lingproc_elem->findnodes('./lp')) {
    my $tstamp = $elem->getAttribute("timestamp");
    $tstamp =~ s/Z$//;
    my $modname = $elem->getAttribute("name");
    if ($modname eq "ixa-pipe-spotlight") {
      my $deb = 0;
      $deb++;
    }
    my $tstamp_str = str2time($tstamp);
    my $tstamp_dt = &get_datetime($tstamp_str);
    my $r = { name => $pre.$modname, tstamp => $tstamp_str, date => $tstamp_dt, secs => 0 };
    push @D, $r;
  }
  return @D;
}

sub open_maybe_bz2 {

  my $fname = shift;

  $fname .= ".bz2" unless -e $fname;
  my $fh;
  if ($fname =~ /\.bz2$/) {
    open($fh, "-|:encoding(UTF-8)", "bzcat $fname") or die "bzcat $fname:$!\n";
  } else {
    open($fh, "<:encoding(UTF-8)", "$fname") or die "$fname:$!\n";
  }
  return $fh;
}

sub rflnames {
  my $fname = shift;

  open(my $fh, $fname) or die "$fname:$!\n";
  my %h;
  while(<$fh>) {
    chomp;
    $h{$_} = 1;
  }
  return %h;
}


sub get_datetime {

  my $date = shift;

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime($date);
  #my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = strptime($date);
  return sprintf "%4d-%02d-%02dT%02d:%02d:%02dZ", $year+1900,$mon+1,$mday,$hour,$min,$sec;

}

