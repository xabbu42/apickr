package Apickr::Common;

use Modern::Perl;
use autodie;
use common::sense;
use List::AllUtils qw/first max min any all sum/;
use YAML;

use Sub::Exporter -setup => {exports => [qw/display add_keys one_element with_progressbar parallelize fix_order/]};

use Coro;
use YAML::Syck qw/LoadFile DumpFile Load Dump/;
use String::ProgressBar;
use Encode qw/encode decode/;
use Encode::Locale;

sub display {
	my ($gen) = @_;

	my $regex = $main::opts->{select} // '\b\B';

	if ($main::opts->{verbose}) {
		$regex = '.';
	} elsif (!$main::opts->{count} || !$main::opts->{select}) {
		$regex .= '|^\.('
			. '((flickr\.)?(title|photoset\.title|contexts\.title|info\.people\.list\.(.*)\.username|datetaken|tags|views|sets|exif))' #flickr photos keys
			. '|photos|count_views|count_comments'                                 #flickr photoset keys
			. '|imageDate|imagePath|keywords|mainRating|name|path|versionNumber'   #aperture image keys
			. '|album|stacks|images'                                               #aperture album keys
			. ')\b';
	}

	my $select = qr/$regex/i;
	my $filter = $main::opts->{filter} ? qr/$main::opts->{filter}/i : qr/./;
	my $walk;
	$walk = sub {
		my ($prefix, $data) = @_;
		my ($result, $result_match) = (undef, 0);
		given (ref $data) {
			when ('HASH') {
				foreach my $key (keys $data) {
					my ($val, $match) = $walk->($prefix . "." . $key, $data->{$key});
					$result_match ||= $match;
					$result->{$key} = $val if defined $val;
				}
			}
			when ('ARRAY') {
				foreach my $val (@$data) {
					my ($new, $match) = $walk->($prefix, $val);
					$result_match ||= $match;
					push @$result, $new if defined $new;
				}
			}
			default {
				$result_match ||= ($data && ($prefix ~~ $filter || $data ~~ $filter));
				$result = $prefix ~~ $select ? $data : undef;
			}
		}
		return ($result, $result_match);
	};

	$gen = with_progressbar($gen) if $main::opts->{count} or $main::opts->{classify};

	my $oldgen = $gen;
	$gen = sub {
		my $row;
		while ($row = $oldgen->()) {
			my ($new, $match) = $walk->('', $row);
			return $new if $new and ($main::opts->{not} ? !$match : $match);
		}
		return $row;
	};

	if ($main::opts->{count}) {
		my $count = 0;
		while (my $row = $gen->()) {
			$count++;
		}
		say $count;
	} elsif ($main::opts->{classify}) {
		my %counts;
		while (my $row = $gen->()) {
			$counts{$row->{$main::opts->{classify}}}++;
		}
		my $maxlen = max map {length $_} keys %counts;
		say sprintf("%-${maxlen}s :   %s", $_, $counts{$_}) foreach sort keys %counts;
	} else {
		say d($_) while $_ = $gen->();
	}
}

sub d {
	my ($row) = @_;
	return Dump(encode_rec($row));
}

sub pd {
	my ($gen) = @_;
	return sub {
		my $next = $gen->();
		say d($next) if $next;
		return $next;
	}
}

sub encode_rec {
	my ($in) = @_;
	given (ref $in) {
		when ('HASH')  { return {map {$_ => encode_rec($in->{$_})} keys %$in} };
		when ('ARRAY') { return [map {encode_rec($_)} @$in] };
		default        { return $in ? encode(locale => $in) : $in }
	}
}

sub fix_order {
	my ($gen) = @_;
	my @got;
	my $last = -1;
	return sub {
		if (@got && $got[0]{num} == $last + 1) {
			$last = $got[0]{num};
			return shift @got;
		}
		do {
			push @got, $gen->();
		} while ($got[-1] && $got[-1]{num} != $last + 1);
		sort {$a->{new} <=> $b->{new}} @got;
		$last = $got[0]{num};
		return shift @got;
	}
}

sub one_element {
	my ($el) = @_;
	return sub {my $r = $el; $el = undef; return $r;};
}

sub with_progressbar {
	my ($gen) = @_;
	my ($bar, $oldtitle, $wrote_newline, $text);
	my $num = 1;
	$| = 1;
	return sub {
		my $val = $gen->();
		unless (defined $val) {
			print "\n" unless $wrote_newline++;
			return undef;
		}
		if ($oldtitle && $val->{photoset}{title} ne $oldtitle or !$bar) {
			print "\n" if $bar;
			$text = $val->{photoset}{title} // $text;
			if (!$text) {
				if ($val->{path}) {
					$text = 'Images';
				} elsif ($val->{images}) {
					$text = 'Albums';
				} elsif ($val->{photos}) {
					$text = 'Photosets';
				} else {
					$text = 'Photos';
				}
			}
			$bar = String::ProgressBar->new(
				max    => $val->{total},
				length => 40,
				text   => encode(locale => sprintf("%20.20s", $val->{photoset}{title} ? $val->{photoset}{title} : $text) . ": "),
			);
		}
		$oldtitle = $val->{photoset}{title};
		$bar->update($num++);
		$bar->info(encode(locale => $val->{title})) if $val->{title};
		$bar->write;
		return $val;
	}
}

sub parallelize {
	my ($gen) = @_;
	my $channel = Coro::Channel->new();
	my $active_coros = 10;
	foreach my $i (0..($active_coros-1)) {
		async {
			my $next;
			do {
				$next = $gen->();
				$channel->put($next) if $next;
			} while ($next);
			$active_coros--;
			$channel->shutdown unless $active_coros;
		}
	}
	return sub { $channel->get };
}

sub add_keys {
	my ($keys, $gen) = @_;
	return sub {
		my $row = $gen->();
		return undef unless $row;
		$row->{$_} = $keys->{$_} foreach keys %$keys;
		return $row;
	}
}

1;
