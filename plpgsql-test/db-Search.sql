
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
(156.22);                             -- ID = 4

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

insert into "Table2"(
  "Field1",       "Field2", "Field3", "Field4", "Field5",        "Field6",            "Field8") values
('Field1 ABC',  'Field2 ABC',  10,      false, '2020-06-06', '2020-06-06 18:10:10', 'T-ENUM'::t_enum),
('Field1 ABCD', 'Field2 ABCD', 11,      false, '2020-06-06', '2020-06-06 18:10:10', 'T-ENUM'::t_enum),
('ABC Field1',  'EFG Field2',  10,      false, '2020-06-06', '2020-06-06 18:10:10', 'T-ENUM'::t_enum),
('abc Field1',  'efg Field2',  101,     false, '2020-06-06', '2020-06-06 18:10:10', 'T-ENUM'::t_enum),
('Zabc Field1', 'xyz Field2',  20,      false, '2020-06-06', '2020-06-06 18:10:10', 'T-ENUM'::t_enum);
