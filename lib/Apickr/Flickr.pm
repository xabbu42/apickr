package Apickr::Flickr;

use Modern::Perl;
use autodie;
use common::sense;
use List::AllUtils qw/first max min any all sum/;
use YAML;

use Sub::Exporter -setup => {exports => [qw/photosets_list photos_list/]};

use Apickr::Common qw/add_keys one_element with_progressbar parallelize/;

use Coro;
use Coro::LWP;
use LWP::Simple qw/get/;
use HTTP::Request::Common;
use Flickr::API2;
use Browser::Open qw/open_browser/;
use XML::LibXML::Simple qw/XMLin/;
use Carp qw/confess cluck/;
use File::Slurp;
use Encode qw/decode_utf8/;
use DateTime::Format::Strptime;
use DateTime::Format::Flexible;
use File::Spec::Functions qw/splitpath catdir/;
use YAML qw/LoadFile DumpFile/;

sub photosets_list {
	my $album = regex_search($main::opts->{album});
	my $id = $main::opts->{ickr_id};
	my $args = {@_};
	my $total = $args->{total};
	delete $args->{total} if $album;
	my $num;
	my $all = flickr_select('photosets.getList', $args);
	return sub {
		my $row;
		return undef if $total && $num >= $total;
		while ($row = $all->()) {
			next if $id && $row->{id} ne $id;
			next if $album && $row->{title} !~ /$album/i;
			$num++;
			return $row;
		}
		return undef;
	}
}

sub get_photo {
	my ($id) = @_;
	my $photo = api('photos.getInfo', 'photo_id' => $id);
	$photo->{total} = $photo->{num} = 1;
	$photo->{datetaken} = $photo->{dates}{taken};
	$photo->{dateupload} = $photo->{dateuploaded};
	delete $photo->{dateuploaded};
	$photo->{datetakengranularity} = $photo->{dates}{takengranularity};
	delete $photo->{dates};
	$photo->{$_} = $photo->{visibility}{$_} foreach keys %{$photo->{visibility}};
	delete $photo->{visibility};
	$photo->{owner} = $photo->{owner}{nsid};
	$photo->{tags} = join ",", map {$_->{_content}} values %{$photo->{tags}};
	return $photo;
}

sub photos_list {
	my ($sets, $gen);
	if ($main::opts->{ickr_id}) {
		eval {
			($gen = one_element(get_photo($main::opts->{ickr_id})))
				|| ($sets = one_element(api('photosets.getInfo', 'photoset_id' => $main::opts->{ickr_id})));
		};
		unless ($gen || $sets) {
			warn $gen;
			warn $sets;
			die "No photo or photoset with id $main::opts->{ickr_id} found.";
		}
	} elsif ($main::opts->{album}) {
		$sets = photosets_list();
	}

	if ($sets) {
		my ($photos, $set, $num);
		my $semaphore = Coro::Semaphore->new();
		$sets = filter_photosets($sets);

		$gen = sub {
			while(1) {
				my $guard = $semaphore->guard();
				return undef if !$set && !($set = $sets->());
				if (!$photos) {
					$photos = add_keys(
						{photoset => $set},
						flickr_select(
							'photosets.getPhotos',
							{photoset_id => $set->{id}, extras => 'date_taken,date_upload,tags,view', @_}
						)
					);
				}
				my $photo = $photos->();
				return $photo if $photo;
				$set = undef; $photos = undef;
			}
		};

		my @photos;
		while (my $photo = $gen->()) {
			push @photos, $photo;
		}
		@photos = sort {$a->{datetaken} cmp $b->{datetaken}} @photos;
		@photos = @photos[0..($main::opts->{num}-1)] if $main::opts->{num};
		my $num = 1;
		$_->{num} = $num++ foreach @photos;
		$gen = sub { return shift @photos };

	} elsif (!$gen) {
		if ($main::opts->{title} && $main::opts->{title} =~ /^[\w\s_]+$/ && $main::opts->{word}) {
			$gen = flickr_select(
				'photos.search',
				{user_id => 'me', sort => 'date-taken-asc', extras => 'date_taken,date_upload,tags,views', text => $main::opts->{title}}
			);
		} else {
			warn "Using complex --title or omitting --word causes slow scan of *all* the photos in the flickr account!\n" if $main::opts->{title};
			$gen = flickr_select(
				'people.getPhotos',
				{user_id => 'me', sort => 'date-taken-asc', extras => 'date_taken,date_upload,tags,views', @_, total => $main::opts->{num}},
			);
		}
	}

	$gen = filter_photos($gen);
	if ($main::opts->{exif} || $main::opts->{contexts} || $main::opts->{info}) {
		$gen = Apickr::Flickr::add_contexts($gen)   if $main::opts->{contexts};
		$gen = Apickr::Flickr::add_photo_info($gen) if $main::opts->{info};
		$gen = Apickr::Flickr::add_exif($gen)       if $main::opts->{exif};
		$gen = parallelize($gen);
	}

	return $gen;
}

sub regex_search {
	my ($search) = @_;
	$search = "\\b$search\\b" if $search and $main::opts->{word};
	return $search ? qr/$search/i : undef;
}

sub filter_photosets {
	my ($gen) = @_;
	my $album = regex_search($main::opts->{album});
	return sub {
		while (1) {
			my $set = $gen->();
			return undef unless $set;
			next unless !$main::opts->{ickr_id} or $set->{ickr_id} == $main::opts->{ickr_id};
			next unless !$album or $set->{title} =~ $album;
			return $set;
		}
	}
}

sub filter_photos {
	my ($gen) = @_;
	my $title   = regex_search($main::opts->{title});
	my $album   = regex_search($main::opts->{album});
	my $ickr_id = $main::opts->{ickr_id};
	return sub {
		while (1) {
			my $photo = $gen->();
			return undef unless $photo;
			next if $title   && $photo->{title}           !~ $title;
			next if $ickr_id && $photo->{id}              ne $ickr_id;
			next if $album   && $photo->{photoset}{title} !~ $album;
			return $photo;
		}
	}
}

sub add_contexts {
	my ($gen) = @_;
	return sub {
		my $photo = $gen->();
		return undef unless $photo;
		my $response = photos('getAllContexts', $photo);
		$photo->{contexts} = [values %$response];
		return $photo;
	}
}

sub add_exif {
	my ($gen) = @_;
	return sub {
		my $photo = $gen->();
		return undef unless $photo;
		my $response = photos('getExif', $photo);
		my $tagtable = Image::ExifTool::GetTagTable('Image::ExifTool::Exif::Main');
		if ($response->{exif} and ref $response->{exif} eq 'ARRAY') {
			foreach my $tag (@{$response->{exif}}) {
				my ($info) = Image::ExifTool::GetTagInfoList($tagtable, $tag->{tag});
				$photo->{exif}{$info->{Name} || $tag->{tag}} = $tag->{raw};
			}
		}
		return $photo;
	}
}

sub add_photo_info {
	my ($gen) = @_;
	return sub {
		my $rec = $gen->();
		return undef unless $rec;
		my $photo = $rec->{flickr} || $rec;
		return $rec unless $photo->{server};
		$photo->{info} = photos('getInfo', $photo);
		$photo->{info}{people}{list} = $photo->{info}{people}{haspeople}
		                               ? ${photos('people.getList', $photo)}{person}
		                               : {};
		return $rec;
	}
}

sub add_photoset_info {
	my ($gen) = @_;
	return sub {
		my $photoset = $gen->();
		return undef unless $photoset;
		$photoset->{info} = photosets('getInfo', $photoset);
		my $photos = flickr_select('photosets.getPhotos', {photoset_id => $photoset->{id}, extras => ''});
		while (my $photo = $photos->()) {
			$photoset->{info}{order}[$photo->{num}-1] = $photo->{id};
			$photoset->{info}{primary} = $photo->{id} if $photo->{isprimary};
		}
		return $photoset;
	}
}

sub fix {
	my $auth = auth();
	my $me = api('people.getInfo', user_id => $auth->{user}{nsid});
	my $profile_html = get($me->{profileurl});
	$profile_html =~ m{<dt>Joined:</dt>\n\s*<dd>(.*?)</dd>}
		or die "Could not get joined date!\n";
	my $joindate = DateTime::Format::Flexible->parse_datetime($1);
	$joindate->add(months => 1);
	my $photo;
	warn "Will only set date_posted after found join date: " . $joindate . "\n";
	my $ickr = parallelize(add_exif(with_progressbar(photos_list())));
	while ($photo = $ickr->()) {
		my %update;
		my $exif_date_str  = $photo->{exif}{DateTimeOriginal} || $photo->{exif}{ModifyDate};
		my $exif_date      = datetime_exif($exif_date_str) if $exif_date_str;
		next unless $exif_date;
		my $datetaken  = datetime_flickr($photo->{datetaken});
		my $dateupload = datetime_flickr($photo->{dateupload});
		if (abs(($exif_date - $datetaken)->in_units('seconds')) > 1) {
			$update{date_taken} = "" . $exif_date;
		}
		if ($main::opts->{'fix-date-upload'}) {
			my $correct_dateupload = $update{date_taken} ? $exif_date->epoch : $datetaken->epoch;
			$correct_dateupload = max($joindate->epoch(), $correct_dateupload);
			if ($dateupload->epoch != $correct_dateupload) {
				$update{date_posted} = $correct_dateupload;
			}
		}
		photos('setDates', $photo, %update)
			if (%update);
	}
}

sub backup {
	my $path = shift() or die "No path for backup file given!\n";
	my $file;
	if ($path eq "-") {
		open $file, '>&:encoding(utf-8)', 'STDOUT';
	} else {
		die "Will not overwrite existing file $path!\n" if -e $path && !$main::opts->{force};
		open $file, ">:encoding(UTF-8)", $path;
	}

	$file->print(Dump($main::opts));
	$file->print(Dump(undef));

	my $photos = parallelize(add_photo_info(with_progressbar(photos_list(extras => ''))));
	my (@photosets_list, %photosets_hash);
	while (my $photo = $photos->()) {
		my $backup = {%$photo, %{$photo->{info}}};
		delete $backup->{info};
		$file->print(Dump($backup));
		if (%{$photo->{photoset}}) {
			my $id = $photo->{photoset}{id};
			push @photosets_list, $id unless $photosets_hash{$id};
			$photosets_hash{$id} ||= {%{$photo->{photoset}}};
			$photosets_hash{$id}{order}[$photo->{num}-1] = $photo->{id};
		}
	}
	$file->print(Dump(undef));

	my $photosets = parallelize(add_photoset_info(with_progressbar(
		@photosets_list ? sub {$photosets_hash{shift @photosets_list}}
		                : photosets_list(),
	)));
	while (my $photoset = $photosets->()) {
		my $backup = {%$photoset, %{$photoset->{info}}};
		delete $photoset->{info};
		$file->print(Dump($backup));
	}
	$file->close();
}

sub restore {
	my %byid;
	my $path = shift() or die "No path for backup file given!\n";
	my ($opts_yaml, $photo_backups_yaml, $photoset_backups_yaml) =
		map {my @res = split "---", $_; shift @res; \@res;} split("--- ~", decode_utf8(read_file($path)));
	local $main::opts = {%$main::opts, %{Load($opts_yaml->[0])}};

	my $photo_backups = parallelize(
		add_photo_info(
			filter_photos(
				with_progressbar(sub { Load(shift $photo_backups_yaml) })
			)
		)
	);

	while (my $old = $photo_backups->()) {
		my $new = $old->{info};

		if (!$new) {
			warn "Photo with id $old->{id} not found\n";
			continue;
		}

		if ($old->{title} ne $new->{title} || $old->{description} ne $new->{description}) {
			photos_selected('setMeta', $old, title => $old->{title}, description => $old->{description});
		}

		my %setdates;
		if ($old->{dates}{posted} ne $new->{dates}{posted}) {
			$setdates{date_posted} = $old->{dates}{posted};
		}
		if ($old->{dates}{taken} ne $new->{dates}{taken}) {
			$setdates{date_taken} = $old->{dates}{taken};
		}
		if ($old->{dates}{takengranularity} ne $new->{dates}{takengranularity}) {
			$setdates{date_taken_granularity} = $old->{dates}{takengranularity};
		}
		if (%setdates) {
			photos_selected('setDates', $old, %setdates);
		}

		if ($old->{safety_level} != $new->{safety_level}) {
			photos_selected('setSafetyLevel', $old, safety_level => $old->{safety_level} + 1);
		}

		if ($old->{license} != $new->{license}) {
			photos_selected('licenses.setLicense', $old, license_id => $old->{license});
		}

		if ($new->{location} && !$old->{location}) {
			photos_selected('geo.removeLocation', $old);
		} else {
			if (any {$old->{location}{$::_} ne $new->{location}{$::_}} qw/longitude latitude context accuracy/) {
				photos_selected('geo.setLocation', $old, lon => $old->{location}{longitude}, lat => $old->{location}{latitude}, context => $old->{location}{context}, accuracy => $old->{location}{accuracy});
			}
			my @fields = qw(contact family friend public);
			if (any {$old->{geoperms}{'is' . $::_} ne $new->{geoperms}{'is' . $::_}} @fields) {
				photos_selected('geo.setPerms', $old, map {'is_' . $_ => $old->{geoperms}{'is' . $_}} @fields);
			}
		}

		if (join(" ", sort keys $old->{tags}) ne join(" ", sort keys $new->{tags})) {
			my $tagstring = join(" ", map {'"' . $_->{raw} . '"'} values $old->{tags});
			photos_selected('setTags', $old, tags => $tagstring);
		}
		if (any {$old->{permission}{$::_} ne $new->{permissions}{$::_}} qw/permaddmeta permcomment/,
			&& any {$old->{visibility}{$::_} ne $new->{visibility}{$::_}} qw/isfamily isfriend ispublic/,
		) {
			photos_selected(
				'setPerms', $old,
				map {'is_' . $_ => $old->{visibility}{'is' . $_}} qw/public friend family/,
				map {'perm_' . $_ => $old->{permissions}{'perm' . $_}} qw/comment addmeta/,
			);
		}

		my ($score_matrix, $allscores) = ({}, {});
		my %weight = (id => 100, content => 3, x => 2, y => 2, w => 1, h => 1);
		foreach my $n (values $old->{notes}) {
			foreach my $m (values $new->{notes}) {
				my $score = sum(map {$weight{$_}} grep {$n->{$_} eq $m->{$_}} qw/id x y h w _content/);
				if ($score > 0) {
					$score_matrix->{$n->{id}}{$m->{id}} = $score;
					$allscores->{$score}++;
				}
			}
		}
		if (!$main::opts->{select} || "notes" =~ /$main::opts->{select}/) {
			foreach my $score (sort {$b <=> $a} keys $allscores) {
				foreach my $n (values $old->{notes}) {
					my $m = first {$score_matrix->{$n->{id}}{$::_->{id}} == $score} values $new->{notes};
					next unless $m;
					delete $old->{notes}{$n->{id}};
					delete $new->{notes}{$m->{id}};
					next unless any {$n->{$::_} ne $m->{$::_}} qw/_content x y w h/;
					photos('notes.edit', $old, note_id => $m->{id}, note_text => $n->{_content}, map {'note_' . $_ => => $n->{$_}} qw/x y w h/);
				}
			}
			foreach my $n (values $old->{notes}) {
				photos('notes.add', $old, note_text => $n->{_content}, map {'note_' . $_ => => $n->{$_}} qw/x y w h/);
			}
			foreach my $m (values $new->{notes}) {
				api('photos.notes.delete', note_id => $m->{id});
			}
		}

		if (!$main::opts->{select} || "people" =~ /$main::opts->{select}/) {
			foreach my $p (values $old->{people}{list}) {
				my $q = $new->{people}{list}{$p->{nsid}};
				my %coords = map { 'person_' . $_ => $p->{$_} } qw/x y h w/;
				if (!$q) {
					photos('people.add', $old, 'user_id' => $p->{nsid}, %coords);
				} elsif (all {!defined $p->{$::_}} qw/x y h w/ and any {defined $q->{$::_}} qw/x y h w/) {
					photos('people.deleteCoords', $old, 'user_id' => $p->{nsid});
				} elsif (any {$p->{$::_} ne $q->{$::_}} qw/x y h w/) {
					photos('people.editCoords', $old, 'user_id' => $p->{nsid}, %coords);
				}
			}
			if ($new->{people}{list}) {
				foreach my $q (values $new->{people}{list}) {
					unless ($old->{people}{list}{$q->{nsid}}) {
						photos('people.delete', $old, 'user_id' => $q->{nsid});
					}
				}
			}
		}
	}

	my $photoset_backups = parallelize(
		add_photoset_info(
			with_progressbar(
				filter_photosets( sub {
					state $i = 0;
					my $res = Load($photoset_backups_yaml->[$i++]);
					return undef unless $res;
					$res->{total} = +@$photoset_backups_yaml;
					$res->{num} = $i;
					return $res;
				})
			)
		)
	);

	while (my $old = $photoset_backups->()) {
		my $new = $old->{info};

		my %update_photos;

		my $oldorder = join(",", @{$old->{order}});
		if ($oldorder ne join(",", @{$new->{order}}) && (!$main::opts->{select} || 'photo_ids' =~ /$main::opts->{select}/)) {
			$update_photos{photo_ids} = $oldorder;
		}
		if ($update_photos{photo_ids} or $old->{primary} ne $new->{primary} && (!$main::opts->{select} || 'primary_photo_id' =~ /$main::opts->{select}/)) {
			$update_photos{primary_photo_id} = $old->{primary};
		}
		if ($update_photos{photo_ids}) {
			photosets('editPhotos', $old, %update_photos);
		} elsif (%update_photos) {
			photosets('setPrimaryPhoto', $old, photo_id => $update_photos{primary_photo_id});
		}

		my %update_meta;
		foreach my $k (qw/title description/) {
			if ($old->{$k} ne $new->{$k} && (!$main::opts->{select} || $k =~ /$main::opts->{select}/)) {
				$update_meta{$k} = $old->{$k};
			}
		}
		if (%update_meta) {
			photosets('editMeta', $old, %update_meta);
		}
	}
}

sub datetime_flickr {
	state $formatter = DateTime::Format::Strptime->new(pattern => '%Y-%m-%d %H:%M:%S');
	my ($val) = @_;
	my $res;
	if ($val =~ /^\d+$/) {
		$res = DateTime->from_epoch(epoch => $val);
	} else {
		($res = $formatter->parse_datetime($val)) or die "Could not parse flickr date '$val'\n";
	}
	$res->set_formatter($formatter);
	return $res;
}

sub datetime_exif {
	state $formatter = DateTime::Format::Strptime->new(pattern => '%Y-%m-%d %H:%M:%S');
	state $parser = DateTime::Format::Strptime->new(pattern => '%Y:%m:%d %H:%M:%S');
	my ($val) = @_;
	my $res = $parser->parse_datetime($val) or die "Could not parse exif date '$val'\n";
	$res->set_formatter($formatter);
	return $res;
}

sub auth {
	my $api = shift;
	my $auth = {};
	my $authpath = catdir($ENV{HOME}, '.apickr.auth');
	if (-e $authpath) {
		$auth = LoadFile($authpath);
	}
	if (!$auth->{token}) {
		my $frob = api('auth.getFrob', api_key => $api->raw->{api_key});
		my $url = $api->raw->request_auth_url('write', $frob);
		open_browser($url);
		say "Please authorize apickr to read and write to your flickr account.";
		say "Press any key to continue.";
		local $| = 0;
		getc;
		$auth = api('auth.getToken', frob => $frob);
		DumpFile($authpath, $auth);
	}
	return $auth;
}

sub api {
	state $api  = Flickr::API2->new({key => '04c25240b84f6f5d32e8e43fd4deb249', secret => 'abef06bf4946a4a2'});
	state $auth = auth($api);
	my $method = shift;
	my $params = {@_};
	$params->{auth_token} = $auth->{token}
		if $auth;
	my $req;
	if (($main::opts->{verbose} || $main::opts->{'dry-run'}) && $method =~ /\.(set|remove|delete|add|edit|upload)/) {
		print "\n", 'flickr.' . $method, "\n", Dump($params), "\n";
		return undef if $main::opts->{'dry-run'};
	} elsif  ($main::opts->{verbose} > 1) {
		print "\n", 'flickr.' . $method, "\n", Dump($params), "\n";
	}

	while(1) {
		eval {
			if ($method eq 'photos.upload') {
				$req = upload_photo($api, $params);
			} else {
				$req = $api->execute_method('flickr.' . $method, {%$params});
			}
		};
		if ($@ && (!$params->{page} || $params->{page} == 1)) {
			if ($@ =~ /HTTP status: (504|500)/) {
				cluck $@;
			} else {
				confess $@;
			}
		} else {
			last;
		}
	};

	if ($req && $req->{stat} eq 'ok') {
		delete $req->{stat};
		if (%$req) {
			my ($first) = keys $req;
			return simplify_response($req->{$first}, $params->{page} ? 0 : 1);
		}
	}
	return undef;
}

sub upload_photo {
	my ($api,$params) = @_;
	my $raw = $api->raw();
	$params = {
		api_key        => $raw->{api_key},
		%$params,
	};
	my $photo = delete $params->{photo};
	$params->{api_sig} = $raw->sign_args($raw->{api_secret}, $params);
	$params->{photo}   = [$photo];
	my $req = POST 'http://api.flickr.com/services/upload/', 'Content_Type' => 'form-data', 'Content' => $params;
	my $response = $raw->do_request($req);
	die("API call failed with HTTP status: " . $response->code . "\n")
		unless $response->code == 200;
	my $content = $response->decoded_content;
	$content = $response->content() unless defined $content;
	my $result = XMLin($content);

	return $result if ($result->{stat} eq 'ok');

	die sprintf("API call failed: \%s (\%s)\n", $result->{err}{msg}, $result->{err}{code});
}

sub photos {
	my ($method, $photo, %args) = @_;
	$args{photo_id} = $photo->{id} if $photo->{id};
	return api('photos.' . $method, %args);
}

sub photos_selected {
	state $semaphore = Coro::Semaphore->new(10);
	state @wait_coros;
	END { $_->join foreach @wait_coros };
	my ($method, $photo, %args) = @_;
	if ($main::opts->{select}) {
		foreach my $key (keys %args) {
			delete $args{$key} unless $key =~ /$main::opts->{select}/;
		}
	}
	if (%args) {
		$semaphore->down();
		my $coro = async {
			my $res = photos($method, $photo, %args);
			$semaphore->up();
			$res;
		};
		push @wait_coros, $coro;
		return $coro;
	}
	return undef;
}

sub photosets {
	my ($method, $photoset, @args) = @_;
	return api('photosets.' . $method, photoset_id => $photoset->{id}, @args);
}

sub simplify_response {
	my ($r, $inarray, $inkey) = @_;
	my $singular = $1 if $inkey && $inkey =~ /^(\w+)s$/;
	given (ref $r) {
		when ('HASH') {
			if (exists $r->{_content} && keys($r) == 1) {
				return decode_utf8($r->{_content});
			} elsif ($singular && keys $r == 1 && defined($r->{$singular})) {
				return simplify_response($r->{$singular}, $inarray,);
			} else {
				return { map {$_ => simplify_response($r->{$_}, $inarray, $_)} keys %$r };
			}
		}
		when ('ARRAY') {
			if ($inarray && all {ref($::_) eq 'HASH' and $::_->{id} || $::_->{nsid}} @$r) {
				return { map {($_->{id} || $_->{nsid}) => simplify_response($_, $inarray)} @$r };
			} else {
				return [map {simplify_response($_, 1)} @$r];
			}
		}
		default {
			return decode_utf8($r);
		}
	}
}

sub flickr_select {
	my ($method, $args) = @_;
	my ($resp, $page, $key, $i, $nextpage_coro);
	my $num = 1;
	my $total = $args->{total};
	delete $args->{total};
	$args->{per_page} = $total // 200;
	my $semaphore = Coro::Semaphore->new();
	return sub {
		my $guard = $semaphore->guard();
		if (!$nextpage_coro and (!$i || (($i >= @{$resp->{$key}} * (3/4)) && (@{$resp->{$key}} == $args->{per_page}) && !$total))) {
			$nextpage_coro = async {
				$args->{page} = ++$page;
				my $resp = api($method, %$args);
				return undef unless $resp;
				my $key = first {ref($resp->{$_}) eq 'ARRAY'} keys %$resp;
				return undef unless $key && @{$resp->{$key}};
				return ($resp, $key);
			}
		}
		if (!$i or $i == @{$resp->{$key}}) {
			if (!$nextpage_coro) {
				return undef;
			}
			($resp, $key) = $nextpage_coro->join();
			$nextpage_coro = undef;
			$i = 0;
		}
		my $res = $resp->{$key}[$i++];
		$res->{total} = $total // $resp->{total};
		$res->{num} = $num++;
		return $res;
	}
}

1;
