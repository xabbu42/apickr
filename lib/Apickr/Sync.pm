package Apickr::Sync;

use Modern::Perl;
use autodie;
use common::sense;
use List::AllUtils qw/first max min any all sum/;
use YAML;
use Coro;

use Apickr::Common qw/fix_order with_progressbar parallelize/;
use Apickr::Flickr;
use Apickr::Aperture;

sub match {
	my $explanation = <<'EOM';
Apickr sorts images by date to match the same images from aperture and flickr.
Mulitple images with the exact same date are not yet supported.
EOM
	my ($ap_gen, $ickr_gen) = @_;

	$ickr_gen = fix_order($ickr_gen);
	my $ickr = $ickr_gen->();
	my ($ap, $last_ap_date);
	my $semaphore = Coro::Semaphore->new();

	return sub {
		my $guard = $semaphore->guard();
		while ($ap = $ap_gen->()) {
			die "\nGot multiple images from Aperture with same date.\n$explanation"
				if $last_ap_date && $ap->{imageDate} eq $last_ap_date;
			$last_ap_date = $ap->{imageDate};
			while ($ickr and %$ickr and $ickr->{datetaken} lt $ap->{imageDate}) {
				my $last = $ickr->{datetaken};
				$ickr = $ickr_gen->();
				die "\nGot multiple images from Flickr with same date.\n$explanation"
					if ($ickr->{datetaken} eq $last);
			}
			if ($ickr and %$ickr and $ickr->{datetaken} eq $ap->{imageDate}) {
				$ap->{flickr} = $ickr;
				$ap->{total} = $ap->{num} = 1 if $main::opts->{ickr_id};
			}
			return $ap unless $main::opts->{ickr_id} && !$ap->{flickr};
		}
		return undef;
	}
}

sub sync {
	my $ap_gen   = Apickr::Aperture::select_images();
	my $ickr_gen;
	{
		local $main::opts = {%$main::opts, 'contexts' => 1, 'info' => 1};
		$ickr_gen = parallelize(with_progressbar(Apickr::Flickr::photos_list(), 'Photos'));
	}
	my $match    = match($ap_gen, $ickr_gen);

	my $sets;
	my $gen = Apickr::Flickr::photosets_list();
	while (my $set = $gen->()) {
		$sets->{$set->{title}} = $set;
	}

	my $uploadstart = time();
	my (%toorder, %didupdate);

	my $sync_all = sub {
		while (1) {
			my $ap;
			do {
				$ap = $match->();
			} while $ap && !$ap->{flickr} && !$main::opts->{upload};
			last unless $ap;
			my $ickr = $ap->{flickr};

			my $settags  = tags_from_ap($ap, $ickr);
			my $setperms = perms_from_ap($ap, $ickr);

			if (!$ickr) {
				# hardcoded default defaults from flickr, should check if user changed them...
				delete $setperms->{perm_comment} if $setperms->{perm_comment} == 3;
				delete $setperms->{perm_addmeta} if $setperms->{perm_addmeta} == 2;
				my $coro = Apickr::Flickr::photos_selected(
					'upload',
					{},
					title       => $ap->{name},
					description => $ap->{caption},
					tags        => $settags,
					(map {$_ => delete $setperms->{$_}} qw/is_family is_friend is_public/),
					photo       => $ap->{path},
					);
				$settags = undef;
				$ickr = $ap->{flickr} = {id => $coro->join()};
				Apickr::Flickr::photos_selected('setDates', $ickr, date_posted => $uploadstart + $ap->{num});
			} else {
				if ($ap->{name} ne $ickr->{title} || $ap->{caption} ne $ickr->{description}) {
					Apickr::Flickr::photos_selected('setMeta', $ickr, title => $ap->{name}, description => $ap->{caption});
				}

				if (defined $settags) {
					Apickr::Flickr::photos_selected('setTags', $ickr, tags => $settags);
				}
			}

			if ($setperms) {
				Apickr::Flickr::photos_selected('setPerms', $ickr, %$setperms);
			}

			my $set;
			foreach my $context (@{$ickr->{contexts}}) {
				if ($context->{title} eq $ap->{album}) {
					$set = $context;
				}
			}
			if (!$set) {
				$set = $sets->{$ap->{album}};
				if (!$set) {
					my $coro = async {
						Apickr::Flickr::photosets('create', {}, 'title' => $ap->{album}, 'primary_photo_id' => $ickr->{id});
					};
					$sets->{$ap->{album}} = $coro;
					$sets->{$ap->{album}} = $set = $coro->join();
				} else {
					if(ref($set) eq 'Coro') {
						$sets->{$ap->{album}} = $set = $set->join();
					}
					Apickr::Flickr::photosets('addPhoto', $set, 'photo_id' => $ickr->{id});
				}
				$didupdate{$set->{id}}++;
			}
			$toorder{$set->{id}}{$ickr->{id}} = $ap->{num};

			return $ap;
		}
	};

	$sync_all = parallelize($sync_all);

	while ($sync_all->()) {};

	foreach my $setid (keys %didupdate) {
		Apickr::Flickr::photosets(
			'reorderPhotos',
			{id => $setid},
			'photo_ids' => join ",", (sort {$toorder{$setid}{$a} <=> $toorder{$setid}{$b}} keys %{$toorder{$setid}}),
		);
	}
}

sub perms_from_ap {
	my ($ap, $ickr) = @_;

	my %keywords;
	$keywords{$_}++ foreach split ",", $ap->{keywords};

	my $num = 0;
	my $perm_comment = 3; # recommended default for flickr
	my $perm_addmeta = 2; # dito
	foreach my $perm (qw/nobody friends_and_family contacts everybody/) {
		$perm_comment = $num if $keywords{'flickr:comment=' . $perm};
		$perm_addmeta = $num if $keywords{'flickr:addmeta=' . $perm};
		$num++;
	}

	if ( (any {$keywords{'flickr:' . $::_} != $ickr->{info}{visibility}{'is' . $::_}} qw/family friend public/)
		 || $perm_comment != $ickr->{info}{permissions}{permcomment}
		 || $perm_addmeta != $ickr->{info}{permissions}{permaddmeta}
		) {
		return {
			(map {'is_' . $_ => 0+$keywords{'flickr:' . $_}} qw/family friend public/),
			'perm_comment' => $perm_comment,
			'perm_addmeta' => $perm_addmeta,
		}
	} else {
		return undef;
	}
}

sub tags_from_ap {
	my ($ap, $ickr) = @_;

	state ($gottags, %rawtags, %cleantags);
	if (!$gottags) {
		my $auth = Apickr::Flickr::auth();
		my $tags = Apickr::Flickr::api('tags.getListUserRaw', user_id => $auth->{user}{nsid});
		foreach my $tag (@{$tags->{tags}}) {
			$rawtags{$tag->{clean}} = $tag->{raw}[0];
			$cleantags{$_} = $tag->{clean} foreach @{$tag->{raw}};
		}
		$gottags = 1;
	}

	my ($changed, $stars, %tags, %keywords, %rating_tags);

	$keywords{$_}++ foreach split ",", $ap->{keywords};
	$tags{$_}++     foreach split /[ ,]/, $ickr->{tags};

	for ($stars = 1; $stars <= min(4, $ap->{mainRating}); $stars++) {
		my $tag = 'aperture:rating=' . $stars . 'ormore';
		$rating_tags{$tag}++;
	}
	$rating_tags{'aperture:rating=' . $ap->{mainRating}}++;

	foreach my $keyword (keys %keywords, keys %rating_tags) {
		if ($keyword !~ /^flickr:/) {
			my $clean = $cleantags{$keyword} // $keyword;
			$changed = $changed || !$tags{$clean};
			$tags{$clean}++;
		}
	}

	foreach my $tag (keys %tags) {
		my $raw = $rawtags{$tag} // $tag;
		if (!$keywords{$raw} && !$rating_tags{$raw}) {
			$changed = 1;
			delete $tags{$tag};
		}
	}

	if ($changed) {
		return join(" ", map {'"' . ($rawtags{$_} // $_) . '"'} sort (keys %tags));
	} else {
		return undef;
	}
}

1;
