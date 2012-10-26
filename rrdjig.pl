#!/usr/bin/perl -w
require 5.008;
use lib qw(/scratch/rrd-1.4.3-test2/lib/perl);
use RRDs;
use strict;
use Getopt::Long 2.25 qw(:config posix_default no_ignore_case);
use Pod::Usage 1.14;
use Data::Dumper;
use Digest::SHA1;

# main loop
my %opt = ();
sub main()
{
    # parse options
    GetOptions(\%opt, 'help|h', 'man', 'noaction|no-action|n',
        'verbose|v','src-tmpl=s','dst-tmpl=s') or exit(1);
    if($opt{help})     { pod2usage(1) }
    if($opt{man})      { pod2usage(-exitstatus => 0, -verbose => 2) }
    my $src = shift @ARGV or pod2usage(1);
    if (not -r $src)   { pod2usage("Reading $src: $!") }
    my $dst = shift @ARGV or pod2usage(1);
    if (not -w $dst)   { pod2usage("Accessing $dst: $!") }

    rrdjig($src,$opt{'src-tmpl'},$dst,$opt{'dst-tmpl'});
}

main;

sub rrd_err_check(){
    my $err = RRDs::error();
    if ($err){
        die "RRD Error: $err\n";
    }
}

# how should the data be fetched from the source
# to provide the best approximation of the original data

sub step_sync ($$){
    my $value = shift;
    my $step = shift;
    return ($value - ($value % $step));
}

sub get_rra_size_map($){
    my $info = shift;    
    my $map = {};
    my $min_start;
    for (my $i=0;;$i++){
        my $cf = $info->{"rra[$i].cf"};
        last if not $cf;
        next if $cf !~ /AVERAGE|MIN|MAX/;
        my $pdp_per_row = $info->{"rra[$i].pdp_per_row"};
        next if $cf =~ /MIN|MAX/ and $pdp_per_row == 1;
        my $rows = $info->{"rra[$i].rows"};
        my $step = $pdp_per_row*$info->{step};
        my $start = step_sync($info->{last_update},$step) - $step*$rows;
        if (not defined $min_start or $start < $min_start) {
            $min_start = $start;
        }
        if (  $map->{$cf}{$pdp_per_row}{rows} || 0 < $rows
            or $map->{$cf}{$pdp_per_row}{start} || 0 < $start ){
            $map->{$cf}{$pdp_per_row} = {
                id   => $i,
                rows => $rows,
                step => $step,
                start => $start
            };
        }
    }
    return ($min_start,$map);
}


sub prep_fetch_tasks ($$){
    my $src_info = shift;
    my $dst_info = shift;
    my ($min_start,$src_size) = get_rra_size_map($src_info);
    my $now = step_sync($src_info->{last_update}, $src_info->{step});
    my $first = step_sync($dst_info->{last_update} , $dst_info->{step});
    if ($min_start > $first ) {
        $first = $min_start;
    }
    print "Search $first to $now\n" if $opt{verbose};
    my $task = {};
    for my $cf (qw(AVERAGE MIN MAX)){
        my $x = $src_size->{$cf};
        my $pointer = $now;
        $task->{$cf} = [];
        for my $pdp_per_row (sort {$a <=> $b} keys %$x){
            my $step = $x->{$pdp_per_row}{step};
            my $new_pointer = $x->{$pdp_per_row}{start};
            print "look $cf $pdp_per_row * $step - $new_pointer\n" if $opt{verbose};
            if ($new_pointer <= $first){
                $new_pointer = step_sync($first,$step);
            }
            if ($new_pointer <= $pointer){
                unshift @{$task->{$cf}}, {
                    start => $new_pointer,
                    end => step_sync($pointer,$step),
                    step => $step
                };
                $pointer = $new_pointer;
            }
            last if $pointer <= $first;
        }
    }
    return ($first,$task);
}

sub fetch_data($$$){
    my $src = shift;
    my $first = shift;
    my $tasks = shift;
    my %data;
    my @tmpl;
    if ($opt{'src-tmpl'}){
        @tmpl = split /:/, $opt{'src-tmpl'};
    }
    my %map;
    for my $cf (keys %$tasks){
        print STDERR "FETCH #### CF $cf #####################################\n" 
            if $opt{verbose};
        for my $t (@{$tasks->{$cf}}){
            my ($start,$step,$names,$array) = RRDs::fetch(
                $src,$cf,'--resolution',$t->{step},
                '--start',$t->{start},'--end',$t->{end}
            );
            my $id = 0;
            if (@tmpl and not %map){
                %map = ( map { ($_,$id++) } @$names );
                for my $key (@tmpl){
                    die "ERROR: src key '$key' is not known in $src. Pick from ".join(':',@$names)."\n"
                        if not exists $map{$key};
                }
            }
            rrd_err_check();
            print STDERR "FETCH: want setp $t->{step} -> got step $step  / want start $t->{start} -> got start $start\n" if $opt{verbose};
            my $now = $start;                        
            while (my $row = shift @$array){
                if (@tmpl){
                    push @{$data{$cf}} , [ $now, $step, [ @$row[@map{@tmpl}] ] ];
                }
                else {
                    push @{$data{$cf}} , [ $now, $step, $row ];
                }
                $now+=$step;
            }
        }
    }
    die "ERROR: no AVERAGE RRA found in src rrd. Enhance me to be able to deal with this!\n"
        if not $data{AVERAGE};
    # if older data is required, generate a fake average entry.
    my $start = $data{AVERAGE}[0][0] - $data{AVERAGE}[0][1];
    if ($start > $first ) {
        my $step = $start - $first;
        unshift @{$data{AVERAGE}}, [ $start, $step, [ map {undef} @{$data{AVERAGE}[0][2]} ] ];
    }
    return (\%data);
}

sub reupdate($$$$){
    my $step = shift;
    my $min_time = shift;
    my $dst = shift;
    my $data = shift;
    my @min;
    my @max;
    my @pending = map { 0 } @{$data->{AVERAGE}[0][2]};
    my $hide_cnt = 0;
    my @up;
    while (my $av = shift @{$data->{AVERAGE}}){
        my $end = $av->[0];
        my $start = $end - $av->[1];
        if (my $av_nx = $data->{AVERAGE}[0]){
            my $start_nx = $av_nx->[0] - $av_nx->[1];
            if ($end > $start_nx){
                $end = $start_nx;
            }
        }
        STEP:
        for (my $t = $start+$step;$t<=$end;$t+=$step){
            my @out = @{$av->[2]};
            # lets see if we a usable a MIN or MAX entry pending
            if ($hide_cnt <= 2 and $av->[1] > $step) {
                for my $cf (qw(MIN MAX)){
                    my $m = $data->{$cf}[0];
                    # CHECK IF MIN/MAX CF EXIST (Vicente Gavara)
                    if (defined $m) {
	                    # drop any MIN/MAX entries which we could not use
	                    while ($m->[0] <= $start) {
	                        print STDERR "# DROP $cf $m->[0], $m->[1]\n" if $opt{verbose};
	                        shift @{$data->{$cf}};
	                        $m = $data->{$cf}[0];
	                    }
	                    my $cend = $m->[0];
	                    my $cstep = $m->[1];
	                    my $crow = $m->[2];
	                    if ($cend >= $t and $cend - $cstep <= $t - $step){
	                        my $row = "$t:".join(':',map {defined $_ ? $_ : 'U'} @{$crow});
	                        if ($cf eq 'MIN'){
	                            @min = @{$crow};
	                        } else {
	                            @max = @{$crow};
	                        }
	                        if ($t > $min_time){
	                            print STDERR ($cf eq 'MIN' ? 'm' : 'M' ) ,$row,"\n" if $opt{verbose};
	                            push @up, $row;
	                        }
	                        $hide_cnt++;
	                        for (my $i = 0; $i <@$crow; $i++){
	                            if (defined $pending[$i]){
	                                if (defined $crow->[$i] and defined $out[$i]){
	                                    my $keep = ($out[$i] - $crow->[$i]);
	#                                   print STDERR " - keep $keep\n" if $opt{verbose};
	                                    $pending[$i] += $keep;
	                                }
	                                else {
	                                    $pending[$i] = undef;
	                                }                                
	                            }
	                        }
	                        shift @{$data->{$cf}};
	                        next STEP;
	                    }
                    }
                }
            }

            # compensate for data not shown while insering fake MIN/MAX entries
            for (my $i = 0; $i < @out; $i++){
                if (defined $out[$i] and defined $pending[$i] and $pending[$i] != 0){
                    my $new = $out[$i] + $pending[$i];
                    if (defined $max[$i] and $new > $max[$i]) {
                        $pending[$i] = $new - $max[$i];
                        $out[$i] = $max[$i];
#                       print STDERR " - maxout $i $out[$i]\n" if $opt{verbose};
                    }
                    elsif (defined $min[$i] and $new < $min[$i]){
                        $pending[$i] = $new - $min[$i];
                        $out[$i] = $min[$i];
#                       print STDERR " - minout $i $out[$i]\n" if $opt{verbose};
                    }
                    else {
                        $pending[$i] = 0;
                        $out[$i] = $new;
#                       print STDERR " - combined $i $out[$i]\n" if $opt{verbose};
                    }
                }
                else {
                    $pending[$i] = 0;
                }
            }
            $hide_cnt = 0;
            # show the result;            
            my $row = "$t:".join(':',map {defined $_ ? $_ : 'U'} @out);
            if ($t > $min_time){
                print STDERR " ",$row,"\n" if $opt{verbose};            
                push @up, $row;
            }
        }
    }
    pop @up; # the last update is most likely one too many ...
    if (@up == 0) {
        warn "WARNING: src has no entries new enough to fill dst\n";
    } else {
        print "$min_time $up[0]\n";       
        RRDs::update($dst,
                     $opt{'dst-tmpl'} ? '--template='.$opt{'dst-tmpl'} : (),
                     @up);
        rrd_err_check();
    }
}

sub set_gauge($$){
    my $dst = shift;
    my $info = shift;
    my @tasks;
    for my $key (keys %$info) {
        if ($key =~ m/^ds\[(.+)\]\.type$/
            and $info->{$key} ne 'GAUGE'){
            print STDERR "DS $1 -> GAUGE\n" if $opt{verbose};
            push @tasks, "--data-source-type=${1}:GAUGE";
        }
        if (@tasks) {
            RRDs::tune($dst,@tasks);
            rrd_err_check();
        }
    }
}

sub unset_gauge($$){
    my $dst = shift;
    my $info = shift;
    my @tasks;
    for my $key (keys %$info) {
        if ($key =~ m/^ds\[(.+)\]\.type$/
            and $info->{$key} ne 'GAUGE'){
            print STDERR "DS $1 -> $info->{$key}\n" if $opt{verbose};
            push @tasks, "--data-source-type=${1}:$info->{$key}";
        }
        if (@tasks) {
            RRDs::tune($dst,@tasks);
            rrd_err_check();
        }
    }
}

sub rrdjig($$$$){
    my $src = shift;
    my $src_tmpl = shift;
    my $dst = shift;
    my $dst_tmpl = shift;
    my $dst_info = RRDs::info($dst);
    rrd_err_check();    
    my $src_info = RRDs::info($src);
    rrd_err_check();
    my ($first,$fetch_tasks) = prep_fetch_tasks($src_info,$dst_info);
    my $updates = fetch_data($src,$first,$fetch_tasks);
    set_gauge($dst,$dst_info);
    reupdate($src_info->{step},$dst_info->{last_update},$dst,$updates);
    unset_gauge($dst,$dst_info);
}


__END__

=head1 NAME

rrdjig - use data from an existing rrd file to populate a new one

=head1 SYNOPSIS

B<rrdjig> [I<options>...] I<src.rrd> I<dest.rrd>

     --man           show man-page and exit
 -h, --help          display this help and exit
     --verbose       talk while you work
     --noaction      just talk don't act
     --src-tmpl=tmpl output template for the source rrd
     --dst-tmpl=tmpl input template for the destination rrd

=head1 DESCRIPTION

In rrdtool, data gets processed immediately upon arrival. This means that
the original data is never stored and it is thus not easily possible to
restructure data at a later stage. In the rrdtool core there are no
functions to modify the base step size nor the number and types of RRAs in a
graceful manner.

The rrdjig tool tries to rebuild the original data as closely as possible
based on the data found in the rrd file. It takes AVERAGE, MIN and MAX RRAs
into account and rebuilds the original data stream such that it can be
re-entered into a fresh rrd file. Depending on the configuration of the new
rrd file the resulting data closely matches the data in the original rrd
file.

If the DS configuration of the new RRD file differs from the original
one the B<--src-tmp> and B<--dest-tmp> options can be used to override
the default order of DS entries.

=head1 BEWARE

There are two warnings you should keep in mind:

=over

=item *

This is NEW CODE, so there may be hidden problem. This this first on your real data before doing any major conversions.

=item *

In my testing there were differences between source and destination which I attribute to
quantization issues especially when switching from one consolidation level to the next one.

=back

=head1 EXAMPLE

F<legacy.rrd> has data for the last two years and F<new.rrd> is still empty
but created with a start data two years in the past. F<legacy.rrd> contains
4 Date Sources (in,out,error,drop) and F<new.rrd> contains 3 data-sources
(myout,myin,overrun). We want to transfer the old 'in' to 'myin' and 'out'
to 'myout' while dropping 'error' and 'drop'.

 rrdjiig --src-tmpl=in:out --dst-tmpl=myin:myout legacy.rrd new.rrd

=head1 COPYRIGHT

Copyright (c) 2010 by OETIKER+PARTNER AG. All rights reserved.

=head1 LICENSE

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.

=head1 AUTHOR

S<Tobi Oetiker E<lt>tobi@oetiker.chE<gt>>

The development of  this tool has been sponsored by L<www.init7.net|http://www.init7.net>.
 
=head1 HISTORY

 2010-02-25 to Initial Version

=cut

# Emacs Configuration
#
# Local Variables:
# mode: cperl
# eval: (cperl-set-style "PerlStyle")
# mode: flyspell
# mode: flyspell-prog
# End:
#
# vi: sw=4 et
