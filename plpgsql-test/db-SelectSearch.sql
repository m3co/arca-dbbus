
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
('a6', 'bc2', 'c3', true);
