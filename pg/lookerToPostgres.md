```mermaid
sequenceDiagram

participant 213070643360888 as c0
participant 213070643360892 as c1
participant 213070643360896 as c2
participant 213070643360900 as c3
participant 213070643360904 as c4
participant 213070643360908 as c5
participant 213070643360912 as c6
participant 213070643360916 as c7
participant 213070643360920 as c8
participant 213070643360924 as c9
participant 213070643360928 as c10
participant 213070643360932 as c11
213070643360888->>server:+SSL REQUEST
server-->>213070643360888:-SSL BACKEND ANSWER: N
213070643360888->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360888:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='affbc4bf')
213070643360888->>server:+PASSWORD MESSAGE password=md5de1ae46649b137ee14e14d8fd5fc6cb6
server-->>213070643360888:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360888:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360888:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360888:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360888:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360888:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360888:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360888:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360888:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360888:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360888:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360888:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360888:-BACKEND KEY DATA pid=97, key=2120775944
server-->>213070643360888:-READY FOR QUERY type=<IDLE>
213070643360888->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360888->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360888->>server:+EXECUTE name='', nb_rows=1
213070643360888->>server:+SYNC
server-->>213070643360888:-PARSE COMPLETE
server-->>213070643360888:-BIND COMPLETE
server-->>213070643360888:-COMMAND COMPLETE command='SET'
server-->>213070643360888:-READY FOR QUERY type=<IDLE>
213070643360888->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360888->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360888->>server:+EXECUTE name='', nb_rows=1
213070643360888->>server:+SYNC
server-->>213070643360888:-PARSE COMPLETE
server-->>213070643360888:-BIND COMPLETE
server-->>213070643360888:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360888:-COMMAND COMPLETE command='SET'
server-->>213070643360888:-READY FOR QUERY type=<IDLE>
213070643360888->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360888->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360888->>server:+DESCRIBE kind='P', name=''
213070643360888->>server:+EXECUTE name='', nb_rows=1
213070643360888->>server:+SYNC
server-->>213070643360888:-PARSE COMPLETE
server-->>213070643360888:-BIND COMPLETE
server-->>213070643360888:-NO DATA
server-->>213070643360888:-EMPTY QUERY RESPONSE
server-->>213070643360888:-READY FOR QUERY type=<IDLE>
213070643360888->>server:+DISCONNECT
213070643360892->>server:+SSL REQUEST
server-->>213070643360892:-SSL BACKEND ANSWER: N
213070643360892->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360892:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='0b76ce86')
213070643360892->>server:+PASSWORD MESSAGE password=md5b82e4b6283fe5694c0199ca058378bb8
server-->>213070643360892:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360892:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360892:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360892:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360892:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360892:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360892:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360892:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360892:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360892:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360892:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360892:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360892:-BACKEND KEY DATA pid=98, key=1329847468
server-->>213070643360892:-READY FOR QUERY type=<IDLE>
213070643360892->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360892->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360892->>server:+EXECUTE name='', nb_rows=1
213070643360892->>server:+SYNC
server-->>213070643360892:-PARSE COMPLETE
server-->>213070643360892:-BIND COMPLETE
server-->>213070643360892:-COMMAND COMPLETE command='SET'
server-->>213070643360892:-READY FOR QUERY type=<IDLE>
213070643360892->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360892->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360892->>server:+EXECUTE name='', nb_rows=1
213070643360892->>server:+SYNC
server-->>213070643360892:-PARSE COMPLETE
server-->>213070643360892:-BIND COMPLETE
server-->>213070643360892:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360892:-COMMAND COMPLETE command='SET'
server-->>213070643360892:-READY FOR QUERY type=<IDLE>
213070643360892->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360892->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360892->>server:+DESCRIBE kind='P', name=''
213070643360892->>server:+EXECUTE name='', nb_rows=1
213070643360892->>server:+SYNC
server-->>213070643360892:-PARSE COMPLETE
server-->>213070643360892:-BIND COMPLETE
server-->>213070643360892:-NO DATA
server-->>213070643360892:-EMPTY QUERY RESPONSE
server-->>213070643360892:-READY FOR QUERY type=<IDLE>
213070643360892->>server:+DISCONNECT
213070643360896->>server:+SSL REQUEST
server-->>213070643360896:-SSL BACKEND ANSWER: N
213070643360896->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360896:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='80e88231')
213070643360896->>server:+PASSWORD MESSAGE password=md59023db05ad8c94976359641cf0ada45a
server-->>213070643360896:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360896:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360896:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360896:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360896:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360896:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360896:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360896:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360896:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360896:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360896:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360896:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360896:-BACKEND KEY DATA pid=99, key=1084480878
server-->>213070643360896:-READY FOR QUERY type=<IDLE>
213070643360896->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360896->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360896->>server:+EXECUTE name='', nb_rows=1
213070643360896->>server:+SYNC
server-->>213070643360896:-PARSE COMPLETE
server-->>213070643360896:-BIND COMPLETE
server-->>213070643360896:-COMMAND COMPLETE command='SET'
server-->>213070643360896:-READY FOR QUERY type=<IDLE>
213070643360896->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360896->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360896->>server:+EXECUTE name='', nb_rows=1
213070643360896->>server:+SYNC
server-->>213070643360896:-PARSE COMPLETE
server-->>213070643360896:-BIND COMPLETE
server-->>213070643360896:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360896:-COMMAND COMPLETE command='SET'
server-->>213070643360896:-READY FOR QUERY type=<IDLE>
213070643360896->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360896->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360896->>server:+DESCRIBE kind='P', name=''
213070643360896->>server:+EXECUTE name='', nb_rows=1
213070643360896->>server:+SYNC
server-->>213070643360896:-PARSE COMPLETE
server-->>213070643360896:-BIND COMPLETE
server-->>213070643360896:-NO DATA
server-->>213070643360896:-EMPTY QUERY RESPONSE
server-->>213070643360896:-READY FOR QUERY type=<IDLE>
213070643360896->>server:+DISCONNECT
213070643360900->>server:+SSL REQUEST
server-->>213070643360900:-SSL BACKEND ANSWER: N
213070643360900->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360900:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='68a07783')
213070643360900->>server:+PASSWORD MESSAGE password=md5136c8b8ec47347f93827ec5d7023199c
server-->>213070643360900:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360900:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360900:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360900:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360900:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360900:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360900:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360900:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360900:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360900:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360900:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360900:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360900:-BACKEND KEY DATA pid=100, key=594556468
server-->>213070643360900:-READY FOR QUERY type=<IDLE>
213070643360900->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360900->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360900->>server:+EXECUTE name='', nb_rows=1
213070643360900->>server:+SYNC
server-->>213070643360900:-PARSE COMPLETE
server-->>213070643360900:-BIND COMPLETE
server-->>213070643360900:-COMMAND COMPLETE command='SET'
server-->>213070643360900:-READY FOR QUERY type=<IDLE>
213070643360900->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360900->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360900->>server:+EXECUTE name='', nb_rows=1
213070643360900->>server:+SYNC
server-->>213070643360900:-PARSE COMPLETE
server-->>213070643360900:-BIND COMPLETE
server-->>213070643360900:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360900:-COMMAND COMPLETE command='SET'
server-->>213070643360900:-READY FOR QUERY type=<IDLE>
213070643360900->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360900->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360900->>server:+DESCRIBE kind='P', name=''
213070643360900->>server:+EXECUTE name='', nb_rows=1
213070643360900->>server:+SYNC
server-->>213070643360900:-PARSE COMPLETE
server-->>213070643360900:-BIND COMPLETE
server-->>213070643360900:-NO DATA
server-->>213070643360900:-EMPTY QUERY RESPONSE
server-->>213070643360900:-READY FOR QUERY type=<IDLE>
213070643360900->>server:+PARSE name='', num_params=0, params_type=, query=SELECT pg_backend_pid()
213070643360900->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360900->>server:+DESCRIBE kind='P', name=''
213070643360900->>server:+EXECUTE name='', nb_rows=0
213070643360900->>server:+SYNC
server-->>213070643360900:-PARSE COMPLETE
server-->>213070643360900:-BIND COMPLETE
server-->>213070643360900:-ROW DESCRIPTION: num_fields=1  ---[Field 01]---  name='pg_backend_pid'  type=23  type_len=4  type_mod=4294967295  relid=0  attnum=0  format=0
server-->>213070643360900:-DATA ROW num_values=1  ---[Value 0001]---  length=3  value='100'
server-->>213070643360900:-COMMAND COMPLETE command='SELECT 1'
server-->>213070643360900:-READY FOR QUERY type=<IDLE>
213070643360900->>server:+PARSE name='', num_params=0, params_type=, query=SELECT VERSION() AS version
213070643360900->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360900->>server:+DESCRIBE kind='P', name=''
213070643360900->>server:+EXECUTE name='', nb_rows=0
213070643360900->>server:+SYNC
server-->>213070643360900:-PARSE COMPLETE
server-->>213070643360900:-BIND COMPLETE
server-->>213070643360900:-ROW DESCRIPTION: num_fields=1  ---[Field 01]---  name='version'  type=25  type_len=65535  type_mod=4294967295  relid=0  attnum=0  format=0
server-->>213070643360900:-DATA ROW num_values=1  ---[Value 0001]---  length=112  value='PostgreSQL 13.3 (Debian 13.3-1.pgdg100+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 8.3.0-6) 8.3.0, 64-bit'
server-->>213070643360900:-COMMAND COMPLETE command='SELECT 1'
server-->>213070643360900:-READY FOR QUERY type=<IDLE>
213070643360900->>server:+PARSE name='', num_params=0, params_type=, query=        SELECT COUNT(*)        FROM pg_type AS t0,             pg_aggregate AS t1,             pg_settings AS t2,             pg_settings AS t3
213070643360900->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360900->>server:+DESCRIBE kind='P', name=''
213070643360900->>server:+EXECUTE name='', nb_rows=0
213070643360900->>server:+SYNC
213070643360904->>server:+SSL REQUEST
server-->>213070643360904:-SSL BACKEND ANSWER: N
213070643360904->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360904:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='a03ee692')
213070643360904->>server:+PASSWORD MESSAGE password=md5cfedb52ae0cafa87a4f1066df3ba6802
server-->>213070643360904:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360904:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360904:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360904:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360904:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360904:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360904:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360904:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360904:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360904:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360904:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360904:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360904:-BACKEND KEY DATA pid=101, key=2854304053
server-->>213070643360904:-READY FOR QUERY type=<IDLE>
213070643360904->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360904->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360904->>server:+EXECUTE name='', nb_rows=1
213070643360904->>server:+SYNC
server-->>213070643360904:-PARSE COMPLETE
server-->>213070643360904:-BIND COMPLETE
server-->>213070643360904:-COMMAND COMPLETE command='SET'
server-->>213070643360904:-READY FOR QUERY type=<IDLE>
213070643360904->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360904->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360904->>server:+EXECUTE name='', nb_rows=1
213070643360904->>server:+SYNC
server-->>213070643360904:-PARSE COMPLETE
server-->>213070643360904:-BIND COMPLETE
server-->>213070643360904:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360904:-COMMAND COMPLETE command='SET'
server-->>213070643360904:-READY FOR QUERY type=<IDLE>
213070643360904->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360904->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360904->>server:+DESCRIBE kind='P', name=''
213070643360904->>server:+EXECUTE name='', nb_rows=1
213070643360904->>server:+SYNC
server-->>213070643360904:-PARSE COMPLETE
server-->>213070643360904:-BIND COMPLETE
server-->>213070643360904:-NO DATA
server-->>213070643360904:-EMPTY QUERY RESPONSE
server-->>213070643360904:-READY FOR QUERY type=<IDLE>
213070643360904->>server:+DISCONNECT
213070643360908->>server:+SSL REQUEST
server-->>213070643360908:-SSL BACKEND ANSWER: N
213070643360908->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360908:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='be3afb9d')
213070643360908->>server:+PASSWORD MESSAGE password=md57b5cfa30f89fc7733c814addcd548c03
server-->>213070643360908:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360908:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360908:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360908:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360908:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360908:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360908:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360908:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360908:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360908:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360908:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360908:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360908:-BACKEND KEY DATA pid=102, key=3089396074
server-->>213070643360908:-READY FOR QUERY type=<IDLE>
213070643360908->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360908->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360908->>server:+EXECUTE name='', nb_rows=1
213070643360908->>server:+SYNC
server-->>213070643360908:-PARSE COMPLETE
server-->>213070643360908:-BIND COMPLETE
server-->>213070643360908:-COMMAND COMPLETE command='SET'
server-->>213070643360908:-READY FOR QUERY type=<IDLE>
213070643360908->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360908->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360908->>server:+EXECUTE name='', nb_rows=1
213070643360908->>server:+SYNC
server-->>213070643360908:-PARSE COMPLETE
server-->>213070643360908:-BIND COMPLETE
server-->>213070643360908:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360908:-COMMAND COMPLETE command='SET'
server-->>213070643360908:-READY FOR QUERY type=<IDLE>
213070643360908->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360908->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360908->>server:+DESCRIBE kind='P', name=''
213070643360908->>server:+EXECUTE name='', nb_rows=1
213070643360908->>server:+SYNC
server-->>213070643360908:-PARSE COMPLETE
server-->>213070643360908:-BIND COMPLETE
server-->>213070643360908:-NO DATA
server-->>213070643360908:-EMPTY QUERY RESPONSE
server-->>213070643360908:-READY FOR QUERY type=<IDLE>
213070643360908->>server:+PARSE name='', num_params=0, params_type=, query=SELECT VERSION() AS version
213070643360908->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360908->>server:+DESCRIBE kind='P', name=''
213070643360908->>server:+EXECUTE name='', nb_rows=0
213070643360908->>server:+SYNC
server-->>213070643360908:-PARSE COMPLETE
server-->>213070643360908:-BIND COMPLETE
server-->>213070643360908:-ROW DESCRIPTION: num_fields=1  ---[Field 01]---  name='version'  type=25  type_len=65535  type_mod=4294967295  relid=0  attnum=0  format=0
server-->>213070643360908:-DATA ROW num_values=1  ---[Value 0001]---  length=112  value='PostgreSQL 13.3 (Debian 13.3-1.pgdg100+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 8.3.0-6) 8.3.0, 64-bit'
server-->>213070643360908:-COMMAND COMPLETE command='SELECT 1'
server-->>213070643360908:-READY FOR QUERY type=<IDLE>
213070643360908->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360908->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360908->>server:+DESCRIBE kind='P', name=''
213070643360908->>server:+EXECUTE name='', nb_rows=1
213070643360908->>server:+SYNC
server-->>213070643360908:-PARSE COMPLETE
server-->>213070643360908:-BIND COMPLETE
server-->>213070643360908:-NO DATA
server-->>213070643360908:-EMPTY QUERY RESPONSE
server-->>213070643360908:-READY FOR QUERY type=<IDLE>
213070643360908->>server:+PARSE name='', num_params=0, params_type=, query=          SELECT pid as id,            query as stmt,            EXTRACT(seconds from query_start - NOW()) as elapsed_time          FROM pg_stat_activity          WHERE usename='docker'
213070643360908->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360908->>server:+DESCRIBE kind='P', name=''
213070643360908->>server:+EXECUTE name='', nb_rows=0
213070643360908->>server:+SYNC
server-->>213070643360908:-PARSE COMPLETE
server-->>213070643360908:-BIND COMPLETE
server-->>213070643360908:-ROW DESCRIPTION: num_fields=3  ---[Field 01]---  name='id'  type=23  type_len=4  type_mod=4294967295  relid=12250  attnum=3  format=0  ---[Field 02]---  name='stmt'  type=25  type_len=65535  type_mod=4294967295  relid=12250  attnum=20  format=0  ---[Field 03]---  name='elapsed_time'  type=701  type_len=8  type_mod=4294967295  relid=0  attnum=0  format=0
server-->>213070643360908:-DATA ROW num_values=3  ---[Value 0001]---  length=2  value='81'  ---[Value 0002]---  length=0  value=''  ---[Value 0003]---  length=-1  value=NULL
server-->>213070643360908:-DATA ROW num_values=3  ---[Value 0001]---  length=3  value='100'  ---[Value 0002]---  length=148  value='        SELECT COUNT(*).        FROM pg_type AS t0,.             pg_aggregate AS t1,.             pg_settings AS t2,.             pg_settings AS t3.'  ---[Value 0003]---  length=9  value='-1.155449'
server-->>213070643360908:-DATA ROW num_values=3  ---[Value 0001]---  length=3  value='102'  ---[Value 0002]---  length=190  value='          SELECT pid as id,.            query as stmt,.            EXTRACT(seconds from query_start - NOW()) as elapsed_time.          FROM pg_stat_activity.          WHERE usename='docker'.'  ---[Value 0003]---  length=8  value='0.002903'
server-->>213070643360908:-COMMAND COMPLETE command='SELECT 3'
server-->>213070643360908:-READY FOR QUERY type=<IDLE>
213070643360912->>server:+SSL REQUEST
server-->>213070643360912:-SSL BACKEND ANSWER: N
213070643360912->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360912:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='14444412')
213070643360912->>server:+PASSWORD MESSAGE password=md59cef18c88b7988d7ac2f215fc1569c62
server-->>213070643360912:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360912:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360912:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360912:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360912:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360912:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360912:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360912:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360912:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360912:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360912:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360912:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360912:-BACKEND KEY DATA pid=103, key=1911102642
server-->>213070643360912:-READY FOR QUERY type=<IDLE>
213070643360912->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360912->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360912->>server:+EXECUTE name='', nb_rows=1
213070643360912->>server:+SYNC
server-->>213070643360912:-PARSE COMPLETE
server-->>213070643360912:-BIND COMPLETE
server-->>213070643360912:-COMMAND COMPLETE command='SET'
server-->>213070643360912:-READY FOR QUERY type=<IDLE>
213070643360912->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360912->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360912->>server:+EXECUTE name='', nb_rows=1
213070643360912->>server:+SYNC
server-->>213070643360912:-PARSE COMPLETE
server-->>213070643360912:-BIND COMPLETE
server-->>213070643360912:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360912:-COMMAND COMPLETE command='SET'
server-->>213070643360912:-READY FOR QUERY type=<IDLE>
213070643360912->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360912->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360912->>server:+DESCRIBE kind='P', name=''
213070643360912->>server:+EXECUTE name='', nb_rows=1
213070643360912->>server:+SYNC
server-->>213070643360912:-PARSE COMPLETE
server-->>213070643360912:-BIND COMPLETE
server-->>213070643360912:-NO DATA
server-->>213070643360912:-EMPTY QUERY RESPONSE
server-->>213070643360912:-READY FOR QUERY type=<IDLE>
213070643360912->>server:+DISCONNECT
213070643360916->>server:+SSL REQUEST
server-->>213070643360916:-SSL BACKEND ANSWER: N
213070643360916->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360916:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='9eb7546f')
213070643360916->>server:+PASSWORD MESSAGE password=md5c7655226306ab14fa17e44dba876a348
server-->>213070643360916:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360916:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360916:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360916:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360916:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360916:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360916:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360916:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360916:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360916:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360916:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360916:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360916:-BACKEND KEY DATA pid=104, key=927987783
server-->>213070643360916:-READY FOR QUERY type=<IDLE>
213070643360916->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360916->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360916->>server:+EXECUTE name='', nb_rows=1
213070643360916->>server:+SYNC
server-->>213070643360916:-PARSE COMPLETE
server-->>213070643360916:-BIND COMPLETE
server-->>213070643360916:-COMMAND COMPLETE command='SET'
server-->>213070643360916:-READY FOR QUERY type=<IDLE>
213070643360916->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360916->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360916->>server:+EXECUTE name='', nb_rows=1
213070643360916->>server:+SYNC
server-->>213070643360916:-PARSE COMPLETE
server-->>213070643360916:-BIND COMPLETE
server-->>213070643360916:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360916:-COMMAND COMPLETE command='SET'
server-->>213070643360916:-READY FOR QUERY type=<IDLE>
213070643360916->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360916->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360916->>server:+DESCRIBE kind='P', name=''
213070643360916->>server:+EXECUTE name='', nb_rows=1
213070643360916->>server:+SYNC
server-->>213070643360916:-PARSE COMPLETE
server-->>213070643360916:-BIND COMPLETE
server-->>213070643360916:-NO DATA
server-->>213070643360916:-EMPTY QUERY RESPONSE
server-->>213070643360916:-READY FOR QUERY type=<IDLE>
213070643360916->>server:+PARSE name='', num_params=0, params_type=, query=select pg_terminate_backend(100)
213070643360916->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360916->>server:+DESCRIBE kind='P', name=''
213070643360916->>server:+EXECUTE name='', nb_rows=0
213070643360916->>server:+SYNC
server-->>213070643360916:-PARSE COMPLETE
server-->>213070643360916:-BIND COMPLETE
server-->>213070643360916:-ROW DESCRIPTION: num_fields=1  ---[Field 01]---  name='pg_terminate_backend'  type=16  type_len=1  type_mod=4294967295  relid=0  attnum=0  format=0
server-->>213070643360916:-DATA ROW num_values=1  ---[Value 0001]---  length=1  value='t'
server-->>213070643360916:-COMMAND COMPLETE command='SELECT 1'
server-->>213070643360916:-READY FOR QUERY type=<IDLE>
server-->>213070643360900:-PARSE COMPLETE
server-->>213070643360900:-BIND COMPLETE
server-->>213070643360900:-ROW DESCRIPTION: num_fields=1  ---[Field 01]---  name='count'  type=20  type_len=8  type_mod=4294967295  relid=0  attnum=0  format=0
server-->>213070643360900:-ERROR RESPONSE  File: 'postgres.c'  Severity: 'FATAL'  Message: 'terminating connection due to administrator command'  Code: '57P01'  Routine: 'ProcessInterrupts'  Line: '3090'
213070643360908->>server:+DISCONNECT
213070643360916->>server:+DISCONNECT
213070643360920->>server:+SSL REQUEST
server-->>213070643360920:-SSL BACKEND ANSWER: N
213070643360920->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360920:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='1ad4dcf7')
213070643360920->>server:+PASSWORD MESSAGE password=md554de511c5219f67a8bbe92da445fa5a2
server-->>213070643360920:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360920:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360920:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360920:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360920:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360920:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360920:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360920:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360920:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360920:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360920:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360920:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360920:-BACKEND KEY DATA pid=105, key=3630332440
server-->>213070643360920:-READY FOR QUERY type=<IDLE>
213070643360920->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360920->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360920->>server:+EXECUTE name='', nb_rows=1
213070643360920->>server:+SYNC
server-->>213070643360920:-PARSE COMPLETE
server-->>213070643360920:-BIND COMPLETE
server-->>213070643360920:-COMMAND COMPLETE command='SET'
server-->>213070643360920:-READY FOR QUERY type=<IDLE>
213070643360920->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360920->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360920->>server:+EXECUTE name='', nb_rows=1
213070643360920->>server:+SYNC
server-->>213070643360920:-PARSE COMPLETE
server-->>213070643360920:-BIND COMPLETE
server-->>213070643360920:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360920:-COMMAND COMPLETE command='SET'
server-->>213070643360920:-READY FOR QUERY type=<IDLE>
213070643360920->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360920->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360920->>server:+DESCRIBE kind='P', name=''
213070643360920->>server:+EXECUTE name='', nb_rows=1
213070643360920->>server:+SYNC
server-->>213070643360920:-PARSE COMPLETE
server-->>213070643360920:-BIND COMPLETE
server-->>213070643360920:-NO DATA
server-->>213070643360920:-EMPTY QUERY RESPONSE
server-->>213070643360920:-READY FOR QUERY type=<IDLE>
213070643360920->>server:+DISCONNECT
213070643360924->>server:+SSL REQUEST
server-->>213070643360924:-SSL BACKEND ANSWER: N
213070643360924->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360924:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='494c352f')
213070643360924->>server:+PASSWORD MESSAGE password=md5919fc9ed056904fa0d8e1dd00625a556
server-->>213070643360924:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360924:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360924:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360924:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360924:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360924:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360924:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360924:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360924:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360924:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360924:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360924:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360924:-BACKEND KEY DATA pid=106, key=3364308023
server-->>213070643360924:-READY FOR QUERY type=<IDLE>
213070643360924->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360924->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360924->>server:+EXECUTE name='', nb_rows=1
213070643360924->>server:+SYNC
server-->>213070643360924:-PARSE COMPLETE
server-->>213070643360924:-BIND COMPLETE
server-->>213070643360924:-COMMAND COMPLETE command='SET'
server-->>213070643360924:-READY FOR QUERY type=<IDLE>
213070643360924->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360924->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360924->>server:+EXECUTE name='', nb_rows=1
213070643360924->>server:+SYNC
server-->>213070643360924:-PARSE COMPLETE
server-->>213070643360924:-BIND COMPLETE
server-->>213070643360924:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360924:-COMMAND COMPLETE command='SET'
server-->>213070643360924:-READY FOR QUERY type=<IDLE>
213070643360924->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360924->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360924->>server:+DESCRIBE kind='P', name=''
213070643360924->>server:+EXECUTE name='', nb_rows=1
213070643360924->>server:+SYNC
server-->>213070643360924:-PARSE COMPLETE
server-->>213070643360924:-BIND COMPLETE
server-->>213070643360924:-NO DATA
server-->>213070643360924:-EMPTY QUERY RESPONSE
server-->>213070643360924:-READY FOR QUERY type=<IDLE>
213070643360924->>server:+PARSE name='', num_params=0, params_type=, query=SELECT 1
213070643360924->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360924->>server:+DESCRIBE kind='P', name=''
213070643360924->>server:+EXECUTE name='', nb_rows=0
213070643360924->>server:+SYNC
server-->>213070643360924:-PARSE COMPLETE
server-->>213070643360924:-BIND COMPLETE
server-->>213070643360924:-ROW DESCRIPTION: num_fields=1  ---[Field 01]---  name='?column?'  type=23  type_len=4  type_mod=4294967295  relid=0  attnum=0  format=0
server-->>213070643360924:-DATA ROW num_values=1  ---[Value 0001]---  length=1  value='1'
server-->>213070643360924:-COMMAND COMPLETE command='SELECT 1'
server-->>213070643360924:-READY FOR QUERY type=<IDLE>
213070643360924->>server:+DISCONNECT
213070643360928->>server:+SSL REQUEST
server-->>213070643360928:-SSL BACKEND ANSWER: N
213070643360928->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360928:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='4b2173db')
213070643360928->>server:+PASSWORD MESSAGE password=md53977c39c5c7cdef7f80e74b256a8ce25
server-->>213070643360928:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360928:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360928:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360928:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360928:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360928:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360928:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360928:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360928:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360928:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360928:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360928:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360928:-BACKEND KEY DATA pid=107, key=1988416582
server-->>213070643360928:-READY FOR QUERY type=<IDLE>
213070643360928->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360928->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360928->>server:+EXECUTE name='', nb_rows=1
213070643360928->>server:+SYNC
server-->>213070643360928:-PARSE COMPLETE
server-->>213070643360928:-BIND COMPLETE
server-->>213070643360928:-COMMAND COMPLETE command='SET'
server-->>213070643360928:-READY FOR QUERY type=<IDLE>
213070643360928->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360928->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360928->>server:+EXECUTE name='', nb_rows=1
213070643360928->>server:+SYNC
server-->>213070643360928:-PARSE COMPLETE
server-->>213070643360928:-BIND COMPLETE
server-->>213070643360928:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360928:-COMMAND COMPLETE command='SET'
server-->>213070643360928:-READY FOR QUERY type=<IDLE>
213070643360928->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360928->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360928->>server:+DESCRIBE kind='P', name=''
213070643360928->>server:+EXECUTE name='', nb_rows=1
213070643360928->>server:+SYNC
server-->>213070643360928:-PARSE COMPLETE
server-->>213070643360928:-BIND COMPLETE
server-->>213070643360928:-NO DATA
server-->>213070643360928:-EMPTY QUERY RESPONSE
server-->>213070643360928:-READY FOR QUERY type=<IDLE>
213070643360928->>server:+DISCONNECT
213070643360932->>server:+SSL REQUEST
server-->>213070643360932:-SSL BACKEND ANSWER: N
213070643360932->>server:+STARTUP MESSAGE version: 3  database=trait_store  extra_float_digits=2  TimeZone=GMT  client_encoding=UTF8  user=docker  DateStyle=ISO
server-->>213070643360932:-AUTHENTIFICATION REQUEST code=5 (MD5 salt='f6d0f51d')
213070643360932->>server:+PASSWORD MESSAGE password=md5b5b322fe9bcc7c242c6b44cc8a7898e8
server-->>213070643360932:-AUTHENTIFICATION REQUEST code=0 (SUCCESS)
server-->>213070643360932:-PARAMETER STATUS name='application_name', value=''
server-->>213070643360932:-PARAMETER STATUS name='client_encoding', value='UTF8'
server-->>213070643360932:-PARAMETER STATUS name='DateStyle', value='ISO, MDY'
server-->>213070643360932:-PARAMETER STATUS name='integer_datetimes', value='on'
server-->>213070643360932:-PARAMETER STATUS name='IntervalStyle', value='postgres'
server-->>213070643360932:-PARAMETER STATUS name='is_superuser', value='on'
server-->>213070643360932:-PARAMETER STATUS name='server_encoding', value='UTF8'
server-->>213070643360932:-PARAMETER STATUS name='server_version', value='13.3 (Debian 13.3-1.pgdg100+1)'
server-->>213070643360932:-PARAMETER STATUS name='session_authorization', value='docker'
server-->>213070643360932:-PARAMETER STATUS name='standard_conforming_strings', value='on'
server-->>213070643360932:-PARAMETER STATUS name='TimeZone', value='GMT'
server-->>213070643360932:-BACKEND KEY DATA pid=108, key=997991029
server-->>213070643360932:-READY FOR QUERY type=<IDLE>
213070643360932->>server:+PARSE name='', num_params=0, params_type=, query=SET extra_float_digits = 3
213070643360932->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360932->>server:+EXECUTE name='', nb_rows=1
213070643360932->>server:+SYNC
server-->>213070643360932:-PARSE COMPLETE
server-->>213070643360932:-BIND COMPLETE
server-->>213070643360932:-COMMAND COMPLETE command='SET'
server-->>213070643360932:-READY FOR QUERY type=<IDLE>
213070643360932->>server:+PARSE name='', num_params=0, params_type=, query=SET application_name = 'PostgreSQL JDBC Driver'
213070643360932->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360932->>server:+EXECUTE name='', nb_rows=1
213070643360932->>server:+SYNC
server-->>213070643360932:-PARSE COMPLETE
server-->>213070643360932:-BIND COMPLETE
server-->>213070643360932:-PARAMETER STATUS name='application_name', value='PostgreSQL JDBC Driver'
server-->>213070643360932:-COMMAND COMPLETE command='SET'
server-->>213070643360932:-READY FOR QUERY type=<IDLE>
213070643360932->>server:+PARSE name='', num_params=0, params_type=, query=
213070643360932->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360932->>server:+DESCRIBE kind='P', name=''
213070643360932->>server:+EXECUTE name='', nb_rows=1
213070643360932->>server:+SYNC
server-->>213070643360932:-PARSE COMPLETE
server-->>213070643360932:-BIND COMPLETE
server-->>213070643360932:-NO DATA
server-->>213070643360932:-EMPTY QUERY RESPONSE
server-->>213070643360932:-READY FOR QUERY type=<IDLE>
213070643360932->>server:+PARSE name='', num_params=0, params_type=, query=SELECT VERSION() AS version
213070643360932->>server:+BIND portal='', name='', num_formats=0, formats=, num_params=0, params=
213070643360932->>server:+DESCRIBE kind='P', name=''
213070643360932->>server:+EXECUTE name='', nb_rows=0
213070643360932->>server:+SYNC
server-->>213070643360932:-PARSE COMPLETE
server-->>213070643360932:-BIND COMPLETE
server-->>213070643360932:-ROW DESCRIPTION: num_fields=1  ---[Field 01]---  name='version'  type=25  type_len=65535  type_mod=4294967295  relid=0  attnum=0  format=0
server-->>213070643360932:-DATA ROW num_values=1  ---[Value 0001]---  length=112  value='PostgreSQL 13.3 (Debian 13.3-1.pgdg100+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 8.3.0-6) 8.3.0, 64-bit'
server-->>213070643360932:-COMMAND COMPLETE command='SELECT 1'
server-->>213070643360932:-READY FOR QUERY type=<IDLE>
213070643360932->>server:+DISCONNECT
```
