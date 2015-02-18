#!/usr/bin/perl

use XML::LibXML;
use Date::Parse;
use File::Basename;
use Getopt::Std;
use Encode;
use JSON;
use Data::Dumper;
use IO::Uncompress::Bunzip2 qw(bunzip2 $Bunzip2Error) ;

use strict;

# usage: perl $0 -f file_with_filename_listings files

sub usage {

  my $str = shift;
  my $bn = basename($0);
  print STDERR "usage: $bn [-t threshold ] [-b batch_name] [-f file_with_filename_listings] [-v] [-w docs.json] [-r docs.json] files\n";
  print STDERR "$str\n" if $str;
  die;
}

my %opts;
my $parser = XML::LibXML->new(); # XML global parser
$parser->keep_blanks(0);

getopts('t:r:w:vf:b:', \%opts);

my $opt_threshold = 5;

my $opt_v = $opts{'v'};
my $batch_name;
my $batch_N = 0;

$opt_threshold = $opts{'t'} if defined $opts{'t'};
if (defined $opt_threshold) {
  &usage("bad threshold") unless $opt_threshold > 0;
}
print STDERR "Using threshold $opt_threshold\n" if $opt_v;

$batch_name = lc($opts{'b'}) if defined $opts{'b'};
if (defined $batch_name) {
  &usage("bad batch name!") unless $batch_name !~ /\d+$/;
  $batch_N = $&;
}

my $Docs = []; # ( { beg=>tstamp, end=>tstamp, modules=>{ name=>string, secs=>int } }, ... )

if ($opts{'r'}) {
  print STDERR "Reading JSON file ".$opts{'r'}."... " if $opt_v;
  my $fh = &open_maybe_bz2($opts{'r'});
  my @T = <$fh>;
  print STDERR "decoding." if $opt_v;
  $Docs = decode_json(join("", @T));
  print STDERR "\n" if $opt_v;
} else {
  my @ifnames;
  my $flist_fname = $opts{'f'};
  if (not defined $flist_fname) {
    &usage("No input files") unless @ARGV;
    foreach (@ARGV) {
      push(@ifnames, decode("UTF-8", $_));
    }
  } else {
    &load_flist($flist_fname, \@ifnames);
  }
  $Docs = &process_docs(\@ifnames);
  print STDERR "Sorting .... " if $opt_v;
  @{ $Docs } = sort { $a->{beg} <=> $b->{beg} } @{ $Docs };
}

if ($opts{'w'}) {
  open(my $fh, ">".$opts{'w'}) or die "Can't create ".$opts{'w'}.":$!\n";
  binmode $fh;
  print $fh encode_json($Docs);
}

exit 1 unless @{ $Docs };

#die Dumper map { [ $_->{beg}, &get_datetime($_->{beg}), $_->{end}, &get_datetime($_->{end})  ] } @Docs;

# batch == { status=>BOK|BDONE beg=>tstamp, end=>tstamp, docs_N => int }


print STDERR "\nClustering " if $opt_v;
my $Batches = &cluster_batches($Docs);
print STDERR "\nDone!\n" if $opt_v;
&print_batches($Batches);
&print_module_stats($Docs);


sub process_docs {

  my ($ifnames) = @_;

  my $doc_N = 0;
  my $Docs = [];
  print STDERR "Processing ". scalar( @{ $ifnames } )." documents: " if $opt_v;
  my %Proc_docs;			# { md5 => 1 }
  foreach ( @{ $ifnames } ) {
    my $fname = $_;
    # docs_wc_naf/processed/2014/5BW1-BXH1-JBVM-Y11G.xml_7f8b76e4dfb40ecbd97ef1bbf611d685.naf.bz2
    my $md5 = substr(basename($fname, ".naf.bz2"), -32);
    #next if $Proc_docs{$md5};	# already processed
    my $fh = &open_maybe_bz2($fname);
    next unless defined $fh;
    # $doc = { beg=>tstamp, end=>tstamp, modules=>{ name=>string, secs=>int } }
    my $doc = &stat_doc($fh);
    next unless defined $doc;
    $Proc_docs{$md5} = 1;
    push @{ $Docs }, $doc;
    $doc_N++;
    print STDERR "... $doc_N" if $opt_v and not ($doc_N % 10000);
  }
  print STDERR "\nProcessed $doc_N documents\n" if $opt_v;
  return $Docs;
}


sub print_batches {
  my $Batches = shift;
  # print batches
  print "* Batches\n\n";
  print "Total: ".scalar @{ $Batches }." batches\n\n";
  print "| btic | etic | begin | end | docs | secs |\n|-|\n";
  foreach my $batch (sort { $a->{'beg'} <=> $b->{'beg'} } @{ $Batches }) {
    my $btic = $batch->{'beg'};
    my $etic = $batch->{'end'};
    my $begin = &get_datetime($btic);
    my $end = &get_datetime($etic);
    my $N = $batch->{'docs_N'};
    my $secs = $etic - $btic;
    print "| $btic | $etic | $begin | $end | $N | $secs |\n";
  }
  print "\n";
}

  # print module stats
  sub print_module_stats {

    my $Docs = shift;

    my $beg = 100000000000;
    my $end = 0;
    my %H;
    my $doc_N = 0;
    my $secstot = 0;		# total counting header times

    foreach my $doc ( @{ $Docs } ) {
      $doc_N++;
      $beg = $doc->{beg} if $doc->{beg} < $beg;
      $end = $doc->{end} if $doc->{end} > $end;
      foreach my $dd ( @{ $doc->{modules} } ) {
	my $name = $dd->{name};
	my $secs =  $dd->{secs};
	$secstot += $secs;
	$H{$name} += $secs;
      }
    }

    my $dtot = $end - $beg;
    print "* Modules\n\n";
    foreach my $name (sort { $H{$a} <=> $H{$b} } keys %H) {
      my $h = $H{$name};
      printf("| %s | %i | %.2f |\n", $name, $H{$name}, 100*$H{$name} / $secstot);
    }
    print "\n\n";
    print "\n** stats\n\n";
    print "docs:$doc_N\n";
    print "processing_secs:$secstot\n";
    print "elapsed_secs:$dtot\n";
    print "beg:".&get_datetime($beg)."\n";
    print "end:".&get_datetime($end)."\n";
  }

sub stat_doc {

  my ($fh) = @_;

  binmode $fh;			# remove utf-ness

  my $doc = undef;
  eval {
    $doc = $parser->parse_fh($fh);
  };
  return undef if $@;

  my $doc_elem = $doc->getDocumentElement;

  my @D;
  foreach my $elem ($doc_elem->findnodes('/NAF/nafHeader/linguisticProcessors')) {
    # { name => module_name, btsamp->timestamp_tics, etsamp->timestamp_tics, secs=>secs}
    push @D, &lingProc_lps($elem);
  }
  return undef unless @D;
  return { beg => $D[0]->{btstamp},
	   end => $D[0]->{etstamp},
	   mod => \@D } if @D == 1;

  @D = sort { $a->{btstamp} <=> $b->{btstamp} } @D;
  my $prev = 0;
  foreach my $r (@D) {
    if ($prev) {
      if (not $r->{secs} ) {
	$r->{secs} = ($r->{etstamp} - $prev);
	$r->{btstamp} = $prev;
      }
    }
    $prev = $r->{etstamp};
  }
  return { beg => $D[0]->{btstamp},
	   end => $D[-1]->{etstamp},
	   modules => \@D } ;
}

sub tstamp_attr {
  my $elem = shift;
  if ($elem->hasAttribute("beginTimestamp") and $elem->hasAttribute("endTimestamp")) {
    # usual case
    return($elem->getAttribute("beginTimestamp"), $elem->getAttribute("endTimestamp"));
  }
  return undef unless $elem->hasAttribute("timestamp");
  return ($elem->getAttribute("timestamp"), $elem->getAttribute("timestamp"));
}

sub lingProc_lps {
  my $lingproc_elem = shift;
  my @D;
  my $pre = $lingproc_elem->getAttribute("layer")."#";
  foreach my $elem ($lingproc_elem->findnodes('./lp')) {
    my $modname = $elem->getAttribute("name");
    my ($Btstamp, $Etstamp) = &tstamp_attr($elem);
    next unless defined $Btstamp;
    # heuristics for modules which do bad.
    # NOTE: this is specific to WC dataset!! (May/June 2014)
    if ($modname eq "corefgraph-en") {
      # timestamp is endTimestamp. Time is local (+2)
      $Btstamp .= "+0200";
      $Etstamp .= "+0200";
    }
    my $Btstamp_tics = str2time($Btstamp);
    my $Etstamp_tics = str2time($Etstamp);
    my $secs = $Etstamp_tics - $Btstamp_tics;
    my $r = { name => $pre.$modname,
	      btstamp => $Btstamp_tics,
	      etstamp => $Etstamp_tics,
	      # bdate => "$Btstamp\t".&get_datetime($Btstamp_tics),
	      # edate => "$Etstamp\t".&get_datetime($Etstamp_tics),
	      secs => $secs };
    push @D, $r;
  }
  return @D;
}

sub open_maybe_bz2 {

  my $fname = shift;

  $fname .= ".bz2" unless -e $fname;
  my $fh = undef;
  if ($fname =~ /\.bz2$/) {
    $fh = new IO::Uncompress::Bunzip2 $fname
      or return undef;
  } else {
    open($fh, "<:encoding(UTF-8)", "$fname") or return undef;
    binmode $fh;
  }
  return $fh;
}

sub rflnames {
  my $fname = shift;

  open(my $fh, $fname) or die "$fname:$!\n";
  my %h;
  while (<$fh>) {
    chomp;
    $h{$_} = 1;
  }
  return %h;
}

# given "tics" (non-leap seconds since 1970) return a date string in GMT timezone

sub get_datetime {

  my $tics = shift;

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=gmtime($tics);
  return sprintf "%4d-%02d-%02dT%02d:%02d:%02d", $year+1900,$mon+1,$mday,$hour,$min,$sec;

}

sub load_flist {
  my ($flist_fname, $ifnames) = @_;
  my $fh = &open_maybe_bz2($flist_fname);
  while (<$fh>) {
    chomp;
    next if /^\s*$/;
    push @{ $ifnames }, $_;
  }
}

##########################################################
# clustering stuff

  #          <---------------->            batch
  #          <---------------->            doc out
  #                    <------>            doc out
  # <---->  <----> <-----> <---->          doc out
  #                               <----->  doc in

  sub bdoc {
    my ($batch, $doc) = @_;
    return $doc->{beg} - $batch->{end};
  }

sub cluster_batches2 {
  my ($Docs, $Batches) = @_;
  my $doc_N = 0;
  foreach my $doc (@{ $Docs }) {
    my @B;
    foreach my $batch ( @{ $Batches } ) {
      my $status = &bdoc($batch, $doc);
      push @B, [$batch, $status] if $status > 0;
    }
    if (not @B) {
      push @{ $Batches }, {
			   beg => $doc->{beg},
			   end => $doc->{end},
			   docs => [ $doc ],
			   docs_N => 1
			  }
    } else {
      @B = sort { $a->[1] <=> $b->[1] } @B;
      my $batch = $B[0]->[0];
      # include doc in batch
      $batch->{end} = $doc->{end};
      push @{ $batch->{docs} }, $doc;
      $batch->{docs_N}++;
    }
  }
  $doc_N++;
  print STDERR "... $doc_N" if $opt_v and not ($doc_N % 10000);
}

# return
#    - the index of the first batch whose "end" is before tstamp (usually document's "beg")
#    - the difference between batch "end" and tstamp

sub bsearch {
  my ($l, $r, $Batches, $tstamp) = @_;
  my $last_ok = $r;
  my $last_diff = $opt_threshold + 1;
  return ($l, $last_diff) if $Batches->[$l]->{end} > $tstamp ;
  while($r > $l) {
    my $mid = int ( ($l + $r) / 2 );
    my $diff = $tstamp - $Batches->[$mid]->{end};
    if ($diff < 0) {
      # doc "beg" is before batch end
      # look at the left
      $r = $mid;
    } else {
      # doc "beg" is after batch end
      # look at the right
      $last_ok = $mid;
      $last_diff = $diff;
      $l = $mid + 1;
    }
  }
  return ($last_ok, $last_diff);
}

sub repos_batch {
  my ($Batches, $batch, $i, $m) = @_;
  for(my $j = $i + 1; $j < $m; $j++) {
    last if $Batches->[$j]->{end} >= $batch->{end};
    $Batches->[$j - 1] = $Batches->[$j];
    $Batches->[$j] = $batch;
  }
}

sub cluster_batches {

  my ($Docs) = @_;
  my $doc_N = 0;

  my $first_doc = $Docs->[0];
  $Batches = [ {
		beg => $first_doc->{beg},
		end => $first_doc->{end},
		docs => [ $first_doc ],
		docs_N => 1
	       } ];

  for(my $k = 1; $k < scalar @{ $Docs }; $k++) {
    my $doc = $Docs->[$k];
    my $m = scalar( @{ $Batches } );
    my ($i, $diff) = &bsearch(0, $m, $Batches, $doc->{beg});
    if ( $diff < $opt_threshold ) {
      # add to batch at position $i and reposition
      my $batch = $Batches->[$i];
      $batch->{end} = $doc->{end};
      push @{ $batch->{docs} }, $doc;
      $batch->{docs_N}++;
      &repos_batch($Batches, $batch, $i, $m);
    } else {
      # create new batch at position $i and reposition
      my $new_batch = {
		       beg => $doc->{beg},
		       end => $doc->{end},
		       docs => [ $doc ],
		       docs_N => 1
		      };
      ($i) = &bsearch(0, $m, $Batches, $doc->{end});
      splice(@{ $Batches }, $i, 0, $new_batch);
      &repos_batch($Batches, $new_batch, $i, $m + 1);
    }
  }
  return $Batches;
}

