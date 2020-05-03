
create table if not exists "Table1"
(
  "ID" serial,
  "Field1" character varying(255),
  "Field2" character varying(255) not null,
  "Field3" character varying(255) default 'default field3',
  "Field4" boolean,
  constraint "Table1_pkey" primary key ("ID")
)
with (
  OIDS=false
);

insert into "Table1"("Field1", "Field2", "Field3", "Field4") values
('a1', 'ab1', null, true),
('a2', 'ab1', 'c1', false),
('a3', 'ab1', null, false),
('a4', 'bc2', 'c2', null),
('a5', 'bc2', null, null),
('a6', 'bc3', 'c3', true);

create type t_enum as enum (
  'T-ENUM');

create table if not exists "Table2"
(
  "ID" serial,
  "Field1" character varying(255),
  "Field2" text,
  "Field3" numeric(15,2),
  "Field4" boolean,
  "Field5" date,
  "Field6" timestamp without time zone,
  "Field7" timestamp with time zone,
  "Field8" t_enum,
  constraint "Table2_pkey" primary key ("ID")
)
with (
  OIDS=false
);

insert into "Table2"("Field1") values
(null);                               -- ID = 1

insert into "Table2"("Field1") values
('Character Varying 255');            -- ID = 2

insert into "Table2"("Field2") values
('Text');                             -- ID = 3

insert into "Table2"("Field3") values
(156.22);            -- ID = 4

insert into "Table2"("Field4") values
(true),                               -- ID = 5
(false);                              -- ID = 6

insert into "Table2"("Field5") values
('2020-02-01');                       -- ID = 7

insert into "Table2"("Field6") values
('2020-02-01 16:17:18');              -- ID = 8

insert into "Table2"("Field7") values
('2020-02-02 18:19:20+03');           -- ID = 9

insert into "Table2"("Field8") values
('T-ENUM');                           -- ID = 10
