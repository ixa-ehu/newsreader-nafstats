#!/usr/bin/perl

use XML::LibXML;
use Date::Parse;
use File::Basename;
use Getopt::Std;
use Encode;
use JSON;
use Data::Dumper;
use IO::Uncompress::Bunzip2 qw(bunzip2 $Bunzip2Error) ;
use Data::Dumper;

use strict;

my $IDLE_TIME = 600; # if greater, count as idle time (10 minutes)

# usage: perl $0 -f file_with_filename_listings files

sub usage {

	my $str = shift;
	my $bn = basename($0);
	print STDERR "usage: $bn [-g] [-F] [-d dataset_name] [-f file_with_filename_listings] [-v] [-w docs.json] [-r docs.json] files\n";
	print STDERR "$str\n" if $str;
	exit 1;
}

my %opts;
my $parser = XML::LibXML->new(); # XML global parser
$parser->keep_blanks(0);

getopts('ht:r:w:vf:d:gFm', \%opts);

# my $Docs = &docs_statistics_mongo($ARGV[0]);
# #print Dumper($Docs);
# &gantt($Docs);
# die;

&usage() if $opts{'h'};

my $opt_gantt = $opts{'g'} // undef;

my $opt_force = $opts{'F'} // undef;

my $dataset_name = $opts{'d'} // undef;

my $opt_threshold = 5;

my $opt_v = $opts{'v'};

$opt_threshold = $opts{'t'} if defined $opts{'t'};
if (defined $opt_threshold) {
	&usage("bad threshold") unless $opt_threshold > 0;
}
print STDERR "Using threshold $opt_threshold\n" if $opt_v;

my $Docs = []; # ( { fname=>"doc.xml", beg=>tstamp, end=>tstamp, modules=>[ {name=>string, secs=>int, host=>hostname, beg=> tics, end=>tics }, ...], ... )

if ($opts{'r'}) {
	print STDERR "Reading JSON file ".$opts{'r'}."... " if $opt_v;
	my $fh = &open_maybe_bz2($opts{'r'});
	my @T = <$fh>;
	print STDERR "decoding." if $opt_v;
	$Docs = decode_json(join("", @T));
	print STDERR "\n" if $opt_v;
} else {
	my $ifnames = [];
	my $flist_fname = $opts{'f'};
	if (not defined $flist_fname) {
		&usage("No input files") unless @ARGV;
		foreach (@ARGV) {
			push(@{ $ifnames }, decode("UTF-8", $_));
		}
	} else {
		$ifnames = &load_flist($flist_fname);
	}
	$Docs = &docs_statistics($ifnames);
	print STDERR "Sorting .... " if $opt_v;
	@{ $Docs } = sort { $a->{beg} <=> $b->{beg} } @{ $Docs };
}

exit 1 unless @{ $Docs };

if ($opts{'w'}) {
	open(my $fh, ">".$opts{'w'}) or die "Can't create ".$opts{'w'}.":$!\n";
	binmode $fh;
	print $fh encode_json($Docs);
}

#die Dumper map { [ $_->{beg}, &get_datetime($_->{beg}), $_->{end}, &get_datetime($_->{end})  ] } @Docs;

# batch == { status=>BOK|BDONE beg=>tstamp, end=>tstamp, docs_N => int }

# print STDERR "\nClustering " if $opt_v;
# my $Batches = &cluster_batches($Docs);
# print STDERR "\nDone!\n" if $opt_v;
# &display_batches($Batches);

# &R_proctime_vector($Docs);

# my  @dd = sort { $b->{end} - $b->{beg} <=> $a->{end} - $a->{beg} } @{ $Docs };
# print join("\n", map { $_->{fname}."\t".($_->{end} - $_->{beg}) } @dd[0..99])."\n";

# die;

# my $DD = [];
# foreach my $d (@{ $Docs } ) {
# 	next unless $d->{fname} eq "finance_subset_naf_output/19961204/234048newsML.txt_8caf21cb783c6c6c446482986b08f797.naf.bz2";
# 	$DD = [$d];
# 	last;
# }
# &display_stats($DD);

&display_stats($Docs);
&gantt($Docs, 0) if $opt_gantt;

sub docs_statistics {

	my ($ifnames) = @_;

	my $doc_N = 0;
	my $Docs = [];
	print STDERR "Processing ". scalar( @{ $ifnames } )." documents: " if $opt_v;
	my %Proc_docs;				# { md5 => 1 }
	foreach my $fname ( @{ $ifnames } ) {
		# docs_wc_naf/processed/2014/5BW1-BXH1-JBVM-Y11G.xml_7f8b76e4dfb40ecbd97ef1bbf611d685.naf.bz2
		my $md5 = substr(basename($fname, ".naf.bz2"), -32);
		next if not $opt_force and $Proc_docs{$md5}; # already processed
		my $fh = &open_maybe_bz2($fname);
		next unless defined $fh;
		# $doc = { beg=>tstamp, end=>tstamp, modules=>{ name=>string, secs=>int } }
		my $doc = &stat_doc($fh);
		next unless defined $doc;
		$doc->{fname} = "$fname";
		$Proc_docs{$md5} = 1;
		push @{ $Docs }, $doc;
		$doc_N++;
		print STDERR "... $doc_N" if $opt_v and not ($doc_N % 10000);
	}
	print STDERR "\nProcessed $doc_N documents\n" if $opt_v;
	return $Docs;
}


sub display_batches {
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

sub R_proctime_vector {

	my $Docs = shift;

	my @T;
	foreach my $doc ( @{ $Docs } ) {
		push @T, $doc->{end} - $doc->{beg};
	}
	print "ptime <- c(".join(",", @T).")\n";
}

# print stats
sub display_stats {

	my ($Docs, $ibeg, $iend) = @_;

	my ($doc_N, $W, $S, $tot_beg, $tot_end, $doc_proctime, $module_proctime, $H) = &compute_stats($Docs, $ibeg, $iend);
	my $elapsed_time = $tot_end - $tot_beg;
	die "No elapsed time" unless $elapsed_time > 0;
	die "No document processing time" unless $doc_proctime > 0;
	die "No module processing time" unless $module_proctime > 0;
	my $idle = &compute_idle($Docs, $ibeg, $iend);
	if ($idle > $elapsed_time) {
		die "Error. Idle time ($idle) is greater than elapsed time ($elapsed_time)\n";
	}
	print "* Modules\n\n";
	foreach my $name (sort { $H->{$a} <=> $H->{$b} } keys %{ $H }) {
		my $h = $H->{$name};
		printf("| %s | %i | %.2f |\n", $name, $H->{$name}, 100*$H->{$name} / $module_proctime);
	}

	my $throughput = sprintf("%.4f", 60 * $doc_N / ($elapsed_time - $idle)); # docs/minutes
	my $throughput_W = sprintf("%.4f", 60 * $W / ($elapsed_time - $idle));
	my $throughput_S = sprintf("%.4f", 60 * $S / ($elapsed_time - $idle));
	my $throughput_noidle = sprintf("%.4f", 60 * $doc_N / $elapsed_time); # docs/minutes
	my $latency = sprintf("%.4f",1/60 * $doc_proctime / $doc_N);  # minutes/docs
	my $latency_W = sprintf("%.10e",1/60 * $doc_proctime / $W);  # minutes/sentences
	my $latency_S = sprintf("%.4f",1/60 * $doc_proctime / $S);  # minutes/words

	print "\n\n";
	print "\n** stats\n\n";
	print "DocN:$doc_N\n";
	print "Words:$W\n";
	print "Sentences:$S\n";
	print "Document processing time (secs): $doc_proctime\n";
	print "Elapsed time (secs): $elapsed_time\n";
	print "Parallelism rate: ".sprintf("%.4f", $doc_proctime / $elapsed_time)."\n";
	print "Idle time (secs): $idle\n";
	print "Throughput (Doc/elapsed_time_minutes, S/min, W/min): $throughput $throughput_S $throughput_W\n";
	print "Throughput (docs) with no idle time (more than $IDLE_TIME secs): $throughput_noidle\n";
	print "Latency (doc_proctime_minutes/DocN proc/S proc/W): $latency $latency_S $latency_W\n";
	print "beg:".&get_datetime($tot_beg)."\n";
	print "end:".&get_datetime($tot_end)."\n";
}

sub compute_stats {
	my ($Docs, $ibeg, $iend) = @_;

	if (not defined $ibeg) {
		$ibeg = 0;
		$iend = scalar @{ $Docs } unless defined $iend;
	} else {
		$iend = scalar @{ $Docs } if $iend > scalar @{$Docs};
	}
	my $W = 0;
	my $S = 0;
	my $tot_beg = 100000000000;
	my $tot_end = 0;
	my $H = {};
	my $doc_proctime = 0;  # document processing time
	my $module_proctime = 0; # module processing time
	my $doc_N = 0;
	for (my $i = $ibeg; $i < $iend; $i++) {
		$doc_N++;
		my $doc = $Docs->[$i];
		$W += $doc->{w};
		$S += $doc->{s};
		$tot_beg = $doc->{beg} if $doc->{beg} < $tot_beg;
		$tot_end = $doc->{end} if $doc->{end} > $tot_end;
		$doc_proctime += $doc->{end} - $doc->{beg};
		foreach my $dd ( @{ $doc->{modules} } ) {
			my $name = $dd->{name};
			my $secs =  $dd->{secs};
			$H->{$name} += $secs;
			$module_proctime += $secs;
		}
	}
	return ($doc_N, $W, $S, $tot_beg, $tot_end, $doc_proctime, $module_proctime, $H);
}

sub compute_idle {

	my ($Docs, $ibeg, $iend) = @_; # Docs are sorted according to doc begining timestamp

	if (not defined $ibeg) {
		$ibeg = 0;
		$iend = scalar @{ $Docs } unless defined $iend;
	} else {
		$iend = scalar @{ $Docs } if $iend > scalar @{$Docs};
	}
	my $N = scalar @{ $Docs };

	my $i = $ibeg;
	my $idle_time = 0;

	while($i < $iend - 1) {
		my $j = $i + 1;
		$j++ while $j < $N and $Docs->[$j]->{end} < $Docs->[$i]->{end};
		if ($j < $N) {
			my $delta = $Docs->[$j]->{beg} - $Docs->[$i]->{end};
			$idle_time += $delta if $delta > $IDLE_TIME;
		}
		$i = $j;
	}
	return $idle_time;
}


sub stat_doc {

	my ($fh) = @_;

	binmode $fh;				# remove utf-ness

	my $doc = undef;
	eval {
		$doc = $parser->parse_fh($fh);
	};
	return undef if $@;

	my $doc_elem = $doc->getDocumentElement;

	my ($w, $s) = &count_words_sentences($doc_elem);
	my @D;
	foreach my $elem ($doc_elem->findnodes('/NAF/nafHeader/linguisticProcessors')) {
		# { name => module_name, btsamp->timestamp_tics, etsamp->timestamp_tics, secs=>secs}
		my @lp = &lingProc_lps($elem);
		push @D, @lp if @lp;
	}
	return undef unless @D;
	return { beg => $D[0]->{beg},
			 end => $D[0]->{end},
			 idle => 0,
			 modules => \@D,
			 w => $w,
			 s => $s } if @D == 1;

	@D = sort { $a->{beg} <=> $b->{beg} } @D;
	my $module_idle = &compute_idle(\@D);
	# my $prev = 0;
	# foreach my $r (@D) {
	# 	if ($prev) {
	# 		if (not $r->{secs} ) {
	# 			$r->{secs} = ($r->{end} - $prev);
	# 			$r->{beg} = $prev;
	# 		}
	# 	}
	# 	$prev = $r->{end};
	# }
	return { beg => $D[0]->{beg},
			 end => $D[-1]->{end},
			 idle => $module_idle, # remove idle time
			 modules => \@D,
			 w => $w,
			 s => $s } ;
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
	return () if $pre eq "deps#"; # TODO
	my %modh;
	foreach my $elem ($lingproc_elem->findnodes('./lp')) {
		my $modname = $elem->getAttribute("name");
		next if $modh{$pre.$modname};
		$modh{$pre.$modname} = 1; # do not count repeated modules
		my $host = $elem->getAttribute("hostname") // "unknown";
		my ($Btstamp, $Etstamp) = &tstamp_attr($elem);
		next unless defined $Btstamp;
		# heuristics for modules which do bad.
		if ($dataset_name eq "WC") {
			# NOTE: this is specific to WC dataset!! (May/June 2014)
			if ($modname eq "corefgraph-en") {
				# timestamp is endTimestamp. Time is local (+2)
				$Btstamp .= "+0200";
				$Etstamp .= "+0200";
			}
		}
		my $Btstamp_tics = str2time($Btstamp);
		my $Etstamp_tics = str2time($Etstamp);
		my $secs = $Etstamp_tics - $Btstamp_tics;
		my $r = { name => $pre.$modname,
				  host => $host,
				  beg => $Btstamp_tics,
				  end => $Etstamp_tics,
				  secs => $secs };
		push @D, $r;
	}
	return @D;
}

sub count_words_sentences {
	my $root = shift;

	my $w = 0;
	my $s = 0;
	my $last_s = undef;
	foreach my $w_elem ($root->findnodes('/NAF/text/wf')) {
		$w++;
		my $ws = $w_elem->getAttribute('sent');
		if ($ws ne $last_s) {
			$s++;
			$last_s = $ws;
		}
	}

	$s = 1 if $w and not defined $last_s;
	return ($w, $s);
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
	my ($flist_fname) = @_;
	my $ifnames = [];
	my $fh = &open_maybe_bz2($flist_fname);
	while (<$fh>) {
		chomp;
		next if /^\s*$/;
		push @{ $ifnames }, $_;
	}
	return $ifnames;
}

##########################################################
# gantt stuff

sub num_chars {

	my ($x, $factor) = @_;

	return int($x * $factor);
}

sub gantt {

    my ($Docs, $agg) = @_;

    if ($agg) {
		my $tsecs = $Docs->[-1]->{end} - $Docs->[0]->{beg};
		&do_gantt($Docs, $tsecs);
    } else {
		foreach my $doc (@{ $Docs }) {
			my $tsecs = $doc->{end} - $doc->{beg};
			&do_gantt([$doc], $tsecs);
		}
    }
}

sub do_gantt {

	my ($Docs, $tsecs) = @_; # ( { beg=>tstamp, end=>tstamp, modules=>[ {name=>string, secs=>int, host=>hostname, beg=> tics, end=>tics }, ...], ... )

	return unless $tsecs > 0;
	print "* gantt\n";
	foreach my $doc (@{ $Docs }) {
		my $max_columns = 300;
		my $factor = $max_columns / $tsecs;
		my $G = {};
		&gantt_doc($doc, $G);
		# print G
		foreach my $n (sort { $G->{$a}->{i} <=> $G->{$b}->{i} } keys %{ $G } ) {
			# $G->{$n} = [ [lead, elapsed], [prev, elapsed], ... ]
			my $prev = 0;
			my $str ;
			foreach my $X (@{ $G->{$n}->{P} }) {
				my ($lead, $elapsed) = @{ $X };
				my $gap = $lead - $prev;
				$prev += $gap + $elapsed;
				my $np = &num_chars($gap, $factor);
				my $ns = &num_chars($elapsed, $factor);
				my $gc = " " x $np;
				my $ec = "X";
				if ($ns > 1) {
					$ec = "<" . "-" x ($ns - 2) . ">";
				}
				$str .= $gc.$ec;
			}
			printf ("%14s | %s\n", $n, $str);
		}
		print "\n";
		foreach my $n (sort { $G->{$a}->{i} <=> $G->{$b}->{i} } keys %{ $G } ) {
			printf ("%14s | %s\n", $n, join(", ", @{ $G->{$n}->{M} }));
		}
		print "\n";
	}
}

sub gantt_doc {

	my ($doc, $G) = @_;

	foreach my $m (sort { $a->{beg} <=> $b->{beg} } @{ $doc->{modules} }) {
		my $h = $G->{$m->{host}};
		if (not defined $h) {
			$G->{$m->{host}} = { i => scalar(keys %{ $G }), P => [], M => [] };
			$h = $G->{$m->{host}};
		}
 		my $lead = $m->{beg} - $doc->{beg};
		my $elapsed = $m->{end} - $m->{beg};
		$elapsed = 0 if $elapsed < 0;
		push @{ $h->{P} }, [$lead, $elapsed] ;
		push @{ $h->{M} }, $m->{name} ;
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
	while ($r > $l) {
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
	for (my $j = $i + 1; $j < $m; $j++) {
		last if $Batches->[$j]->{end} >= $batch->{end};
		$Batches->[$j - 1] = $Batches->[$j];
		$Batches->[$j] = $batch;
	}
}

sub cluster_batches {

	my ($Docs) = @_;
	my $doc_N = 0;

	my $first_doc = $Docs->[0];
	my $Batches = [ {
					 beg => $first_doc->{beg},
					 end => $first_doc->{end},
					 docs => [ $first_doc ],
					 docs_N => 1
					} ];

	for (my $k = 1; $k < scalar @{ $Docs }; $k++) {
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

##################################################################
# read JSON from mongo log

sub docs_statistics_mongo {

	my ($ifname) = @_;
	my $fh = &open_maybe_bz2($ifname);
	my @T = ("[\n");
	while (<$fh>) {
		s/ObjectId\(([^\)]+)\)/$1/;
		s/ISODate\(([^\)]+)\)/$1/;
		$_ .= "," if /^\}$/;
		push @T, $_;
	}
	$T[-1] =~ s/\,$//;
	push @T, "]\n";
	my $Mongo_out = decode_json(join("", @T));
	my $H = {};
	foreach my $mout (@{ $Mongo_out }) {
		my $doc_id = $mout->{doc_id} ;
		my $tstamp = str2time($mout->{timestamp});
		my $h = $H->{$doc_id};
		if (not defined $h) {
			$H->{$doc_id} = { module => {}, beg => $tstamp, end => $tstamp};
			$h = $H->{$doc_id};
		}
		$h->{beg} = $tstamp if $tstamp < $h->{beg};
		$h->{end} = $tstamp if $tstamp > $h->{end};
		my $mid = $mout->{module_id} // $mout->{tag};
		my $module = $h->{module}->{$mid};
		if (not defined $module) {
			$h->{module}->{$mid} = { beg => $tstamp, end => $tstamp };
			$module = $h->{module}->{$mid};
		}

		$module->{$mout->{tag}} = $tstamp;
		$module->{beg} = $tstamp if $tstamp < $module->{beg};
		$module->{end} = $tstamp if $tstamp > $module->{end};
		#$module->{$mout->{hostname} } = 1;
		$module->{hostname} = $mout->{hostname};
	}
	#die Dumper($H->{"530S-4081-JCBD-8510.xml_f502d3593b7f299a18080e398438f84e.naf_9d67524b23995ffe4a8460d7d0dc4e21"});

	# split hostnames into CPUs
	while (my ($docid, $h) = each % { $H }) {
		# sort modules accroding to begin timestamp
		my $hends;
		my $hm = $h->{module};
		foreach my $mod (sort { $hm->{$a}->{beg} <=> $hm->{$b}->{beg} } keys %{ $hm } ) {
			my $module = $hm->{$mod} ;
			# locate index i such that $module->{beg} < $hends->{hostname}->[i]
			my $i = 0;
			my $ends = $hends->{$module->{hostname}};
			if (not defined $ends) {
				$hends->{$module->{hostname}} = []; $ends = $hends->{$module->{hostname}};
			}
			;
			$i++ while($i < @{ $ends } and $module->{beg} <= $ends->[$i] );
			$ends->[$i] = $module->{end};
			$i++;
			$module->{hostname} .= "#$i";
		}
	}
	#die Dumper($H->{"530S-4081-JCBD-8510.xml_f502d3593b7f299a18080e398438f84e.naf_9d67524b23995ffe4a8460d7d0dc4e21"});

	# create Docs array
	my $Docs;
	while (my ($docid, $h) = each % { $H }) {
		my $d = { beg=> $h->{beg}, end=> $h->{end}, modules => [] };
		my $hm = $h->{module};
		foreach my $mod (sort { $hm->{$a}->{beg} <=> $hm->{$b}->{beg} } keys %{ $hm } ) {
			my $module = $hm->{$mod} ;
			push @{ $d->{modules} }, { name => $mod, secs => $module->{end} - $module->{beg},
									   host => $module->{hostname},
									   beg => $module->{beg}, end => $module->{end} };
		}
		push @{ $Docs }, $d;
	}
	return $Docs;
}
