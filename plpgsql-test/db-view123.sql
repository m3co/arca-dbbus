
create or replace function send_jsonrpc(request json)
  returns void
  language 'plpgsql' volatile
as $$
begin
  perform pg_notify('jsonrpc', request::text);
end;
$$;

create table if not exists "_Table1"
(
  "ID" serial,
  "Field1" character varying(255),
  "Field2" character varying(255) not null,
  "Field3" character varying(255) default 'default field3',
  "Field4" character varying(255) not null default 'default field4',
  constraint "_Table1_pkey" primary key ("ID")
)
with (
  OIDS=false
);

create table if not exists "_Table2"
(
  "ID" serial,
  "Field5" character varying(255),
  "Field6" character varying(255) not null,
  "Field7" character varying(255) default 'default field7',
  "Field8" character varying(255) not null default 'default field8',
  constraint "_Table2_pkey" primary key ("ID")
)
with (
  OIDS=false
);

create table if not exists "_Table3"
(
  "ID" serial,
  "Field9" character varying(255),
  "Field10" character varying(255) not null,
  "Field11" character varying(255) default 'default field11',
  "Field12" character varying(255) not null default 'default field12',
  constraint "_Table3_pkey" primary key ("ID")
)
with (
  OIDS=false
);

-- Esto es nuevo... Hay que documentarlo de algúna u otra forma
create or replace function fixlastval_table1()
  returns trigger
  language plpgsql volatile as
$$
begin
  perform setval('"_Table1_ID_seq"', new."ID");
  return new;
end;
$$;

drop trigger if exists "fixlastval_Table1_after" on "_Table1" cascade;
create trigger "fixlastval_Table1_after"
  after insert on "_Table1"
  for each row
  execute procedure fixlastval_table1();

create or replace function fixlastval_table2()
  returns trigger
  language plpgsql volatile as
$$
begin
  perform setval('"_Table2_ID_seq"', new."ID");
  return new;
end;
$$;

drop trigger if exists "fixlastval_Table2_after" on "_Table2" cascade;
create trigger "fixlastval_Table2_after"
  after insert on "_Table2"
  for each row
  execute procedure fixlastval_table2();

create or replace function fixlastval_table3()
  returns trigger
  language plpgsql volatile as
$$
begin
  perform setval('"_Table3_ID_seq"', new."ID");
  return new;
end;
$$;

drop trigger if exists "fixlastval_Table3_after" on "_Table3" cascade;
create trigger "fixlastval_Table3_after"
  after insert on "_Table3"
  for each row
  execute procedure fixlastval_table3();


create or replace view "Table1-Table2-Table3"("ID1-ID2-ID3",
    "Field1", "Field2", "Field3", "Field4",
    "Field5", "Field6", "Field7", "Field8",
    "Field9", "Field10", "Field11", "Field12") as (
  select
    "_Table1"."ID"::text || "_Table2"."ID"::text || "_Table3"."ID"::text as "ID1-ID2-ID3",
    "Field1", "Field2", "Field3", "Field4",
    "Field5", "Field6", "Field7", "Field8",
    "Field9", "Field10", "Field11", "Field12"
  from "_Table1", "_Table2", "_Table3"
);
