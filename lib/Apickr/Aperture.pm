package Apickr::Aperture;

use Modern::Perl;
use autodie;
use common::sense;
use List::AllUtils qw/first max min any all sum/;
use YAML;

use Sub::Exporter -setup => {exports => [qw/select_images select_albums add_exif/]};

use DBI;
use Image::ExifTool qw/ImageInfo/;
use DateTime;
use DateTime::Format::Epoch;
use DateTime::Format::Strptime;
use File::Spec::Functions qw/splitpath catdir/;
use Carp;

sub select_images {
	return aperture_select(
		($main::opts->{select} ? 'RKVersion.uuid AS id,*,' : '')
		. "RKFolder.name AS album,RKVersion.name,imagePath,mainRating,versionNumber,RKVersion.imageDate,"
		. "(SELECT GROUP_CONCAT(name) FROM RKKeyword JOIN RKKeywordForVersion on keywordId = RKKeyword.modelId WHERE versionId = RKVersion.modelId) AS keywords,"
		. "(SELECT stringProperty FROM p.RKUniqueString JOIN p.RKIptcProperty ON p.RKUniqueString.modelId = p.RKIptcProperty.stringId WHERE p.RKIptcProperty.versionId = RKVersion.modelId AND p.RKIptcProperty.propertyKey = 'Caption/Abstract') as caption,"
		. "(SELECT stringProperty FROM p.RKUniqueString JOIN p.RKIptcProperty ON p.RKUniqueString.modelId = p.RKIptcProperty.stringId WHERE p.RKIptcProperty.versionId = RKVersion.modelId AND p.RKIptcProperty.propertyKey = 'ObjectName') as title"
	);
}

sub select_albums {
	return aperture_select(
		($main::opts->{select} ? 'RKVersion.uuid AS id,*,' : '')
		. "RKFolder.name AS album,"
		. "COUNT(DISTINCT COALESCE(RKVersion.stackUuid,RKVersion.uuid)) AS stacks,"
		. "COUNT(DISTINCT RKVersion.uuid) AS images",
		"GROUP BY RKVersion.projectUuid",
	);
}

sub aperture_select {
	state $calls;

	if ($calls++) {
		confess "APICKR::Aperture::select called more than once";
	}
	my ($select, $add) = @_;
	$add = "" unless $add;
	my $dbpath = catdir($main::opts->{path}, 'Database', 'Library.apdb');
	my $dbh = DBI->connect("dbi:SQLite:dbname=$dbpath","", "", { RaiseError => 1, AutoCommit => 0, ReadOnly => 1});
	END {
		if ($dbh) {
			$dbh->rollback; $dbh->disconnect
		};
	}
	{
		local $dbh->{AutoCommit} = 1;
		$dbh->do('ATTACH DATABASE ? AS p;', undef, catdir($main::opts->{path}, 'Database', 'Properties.apdb'))
			or die $dbh->errstr;
	}
	my $sql =
		"FROM RKVersion"
		. " JOIN RKFolder ON RKFolder.uuid = RKVersion.projectUuid"
		. " JOIN RKMaster ON RKVersion.masterUuid = RKMaster.uuid"
		. " WHERE RKVersion.isInTrash = 0 AND RKVersion.versionNumber > 0 AND RKFolder.folderType = 2 "
		. ($main::opts->{album} ? "AND RKFolder.name REGEXP ? " : '')
		. ($main::opts->{title} ? "AND RKVersion.name REGEXP ? " : '')
		. ($main::opts->{ap_id} ? "AND (RKFolder.uuid = ? OR RKVersion.uuid = ?) " : '');
	my $count = ($add =~ /GROUP BY ([\w.]+)/) ? 'DISTINCT ' . $1 : '*';
	my $limit = $main::opts->{num} ? " LIMIT ? " : '';
	my $csth = $dbh->prepare("SELECT COUNT($count) " . $sql . $limit);
	my $sth = $dbh->prepare("SELECT $select " . $sql . $add . " ORDER BY RKVersion.imageDate " . $limit);
	my $param = 1;
	if ($main::opts->{album}) {
		$sth->bind_param($param, $main::opts->{album});
		$csth->bind_param($param++, $main::opts->{album});
	}
	if ($main::opts->{title}) {
		$sth->bind_param($param, $main::opts->{title});
		$csth->bind_param($param++, $main::opts->{title});
	}
	if ($main::opts->{ap_id}) {
		foreach (0..1) {
			$sth->bind_param($param, $main::opts->{ap_id});
			$csth->bind_param($param++, $main::opts->{ap_id});
		}
	}
	if ($main::opts->{num}) {
		$sth->bind_param($param, $main::opts->{num});
		$csth->bind_param($param, $main::opts->{num});
	}

	$csth->execute();
	my ($total) = @{$csth->fetchrow_arrayref};
	my $num = 1;

	$sth->execute();

	return sub {
		return undef unless $sth;
		my $row = $sth->fetchrow_hashref;
		if (!$row) {
			$sth = undef;
			return undef;
		}
		if ($row->{imagePath}) {
			my ($d,$dir,$name) = splitpath($row->{imagePath});
			my $prev = catdir($main::opts->{path}, 'Previews', $dir, $row->{uuid}, $row->{name} . ".jpg");
			$row->{path} = -e $prev ? $prev : catdir($main::opts->{path}, 'Masters', $row->{imagePath});
		}
		if ($row->{imageDate}) {
			$row->{imageDate} = datetime_aperture($row->{imageDate});
		}
		$row->{total} = $total;
		$row->{num} = $num++;
		return $row;
	}
}

sub add_exif {
	my ($gen) = @_;
	return sub {
		my $row = $gen->();
		return undef unless $row;
		$row->{exif} = ImageInfo($row->{path});
		return $row;
	}
}

sub datetime_aperture {
	state $formatter = DateTime::Format::Strptime->new(pattern => '%Y-%m-%d %H:%M:%S');
	state $macepoch  = DateTime::Format::Epoch->new(
		epoch => DateTime->new(year => 2001, month => 1, day => 1),
	);
	state $timezone  = DateTime::TimeZone->new('name' => 'local');
	my ($val) = @_;
	my $res = $macepoch->parse_datetime(shift) or die "Could not parse aperture date '$val'\n";
	$res->set_formatter($formatter);
	$res->set_time_zone($timezone);
	return $res;
}

1;
