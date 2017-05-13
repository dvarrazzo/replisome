\set VERBOSITY terse
\pset format unaligned

-- predictability
SET synchronous_commit = on;

drop schema if exists s1 cascade;
drop schema if exists s2 cascade;
drop schema if exists s3 cascade;

create schema s1;
create table s1.t (id int primary key, data text);
create table s1.u (id int primary key, data text);
create schema s2;
create table s2.t (id int primary key, data text);
create table s2.u (id int primary key, data text);
create schema s3;
create table s3.t (id int primary key, data text);


select slot_create();

insert into s1.t values (1, 's1.t');
insert into s1.u values (1, 's1.u');
insert into s2.t values (2, 's2.t');
insert into s2.u values (2, 's2.u');
insert into s3.t values (3, 's3.t');

select data from slot_peek('include', '{"table": "t"}');
select data from slot_peek('include', '{"schema": "s1"}');
select data from slot_peek('include', '{"schemas": "s[23]"}');
select data from slot_peek('include', '{"schema": "s1", "table": "u"}');
select data from slot_peek('exclude', '{"schema": "s1"}');
select data from slot_peek('exclude', '{"schemas": "s[23]"}');
select data from slot_peek('exclude', '{"schemas": "s."}', 'include', '{"table": "u"}');


select slot_drop();
