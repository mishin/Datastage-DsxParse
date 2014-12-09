#!/usr/bin/perl
use v5.10;
use DBI;
$db = DBI->connect( "dbi:SQLite:dbname=meta_dsx.db", "", "" )
  ; # Подключаемся к базе данных. Если файла users.db не существует, то он будет создан автоматически

#$db->{unicode} = 1;
#$db->do("create table users (user_name text);"); # Создаем новую таблицу в базе данных
sub add_user {
    my $user  = 'mishin';
    my $query = $db->do("INSERT INTO users VALUES('$user')");
    $query > 0
      ? print "$user added\n"
      : print "$user not added\n"
      ; # Если в результате запроса затронуто больше 0 рядов, значит запрос выполнен успешно, а если нет, то неудачно.
}

sub select_user {
    my $query = $db->prepare("SELECT * FROM users WHERE (user_name LIKE 'm%')")
      ;    # Формируем запрос на выборку
    $query->execute()
      or die( $db->errstr )
      ; # Выполняем запрос. В случае неаозможности выполнения запроса умираем с выводом причины
}

sub select_user_short {
    ($query) = $db->selectrow_array(
        "SELECT count(*) FROM users WHERE (user_name LIKE 'm%')");
    return $query;
}

sub show_user {
    my $query = shift;
    while ( ($user) = $query->fetchrow_array() ) {
        print $user. "\n";
    }
}

say select_user_short();
#show_user($query);
$db->disconnect;
