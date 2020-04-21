
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

create or replace view "Table1-Table2"("ID1-ID2",
    "Field1", "Field2", "Field3", "Field4",
    "Field5", "Field6", "Field7", "Field8") as (
  select
    "_Table1"."ID"::text || '-' || "_Table2"."ID"::text as "ID1-ID2",
    "Field1", "Field2", "Field3", "Field4",
    "Field5", "Field6", "Field7", "Field8"
  from "_Table1", "_Table2"
);

create or replace function action_process_table1_table2()
  returns trigger
  language 'plpgsql' volatile
as $$
declare
  r record;
  id1 integer;
  id2 integer;
  ids text[];
begin
  if tg_op = 'INSERT' then
    if new."ID1-ID2" is null then
      id1 := nextval('"_Table1_ID_seq"');
      id2 := nextval('"_Table2_ID_seq"');
      new."ID1-ID2" = id1::text || '-' || id2::text;
    else
      select regexp_matches(new."ID1-ID2", '(\d+)-(\d+)') into ids;
      id1 := ids[1]::integer;
      id2 := ids[2]::integer;
    end if;
    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(t) as "Row"
      from (
        select
          tg_table_name as "Source",
          '_Table1' as "Target",
          current_database() as "Db",
          true as "Prime"
        ) as ctx, (
          select
            id1 as "ID",
            case when new."Field1" is null -- null,     not default
              then null
              else new."Field1"
            end as "Field1",
            case when new."Field2" is null -- not null, not default
              then null
              else new."Field2"
            end as "Field2",
            case when new."Field3" is null -- null,     default
              then 'default field3' -- repeat default value
              else new."Field3"
            end as "Field3",
            case when new."Field4" is null -- not null, default
              then 'default field4' -- repreat default value
              else new."Field4"
            end as "Field4"
        ) t
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;

    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(t) as "Row"
      from (
        select
          tg_table_name as "Source",
          '_Table2' as "Target",
          current_database() as "Db",
          true as "Prime"
        ) as ctx, (
          select
            id2 as "ID",
            case when new."Field5" is null -- null,     not default
              then null
              else new."Field5"
            end as "Field5",
            case when new."Field6" is null -- not null, not default
              then null
              else new."Field6"
            end as "Field6",
            case when new."Field7" is null -- null,     default
              then 'default field7' -- repeat default value
              else new."Field7"
            end as "Field7",
            case when new."Field8" is null -- not null, default
              then 'default field8' -- repreat default value
              else new."Field8"
            end as "Field8"
        ) t
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;
    return new;
  end if;

  if tg_op = 'UPDATE' then
    select regexp_matches(new."ID1-ID2", '(\d+)-(\d+)') into ids;
    id1 := ids[1]::integer;
    id2 := ids[2]::integer;

    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(t) as "Row",
        row_to_json(h) as "PK"
      from (
        select
          tg_table_name as "Source",
          '_Table1' as "Target",
          current_database() as "Db",
          true as "Prime"
        ) as ctx, (
          select
            id1 as "ID",
            case when
              (new."Field1" <> old."Field1") or
              (old."Field1" is null and new."Field1" is not null) or
              (new."Field1" is null and old."Field1" is not null) -- null,     not default
              then new."Field1"
              else old."Field1"
            end as "Field1",
            case when
              (new."Field2" <> old."Field2") or
              (old."Field2" is null and new."Field2" is not null) or
              (new."Field2" is null and old."Field2" is not null) -- not null, not default
              then new."Field2"
              else old."Field2"
            end as "Field2",
            case when
              (new."Field3" <> old."Field3") or
              (old."Field3" is null and new."Field3" is not null) or
              (new."Field3" is null and old."Field3" is not null) -- null,     default
              then new."Field3"
              else old."Field3"
            end as "Field3",
            case when
              (new."Field4" <> old."Field4") or
              (old."Field4" is null and new."Field4" is not null) or
              (new."Field4" is null and old."Field4" is not null) -- not null, default
              then new."Field4"
              else old."Field4"
            end as "Field4"
        ) t, (
          select
            id1 as "ID"
        ) h
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;

    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(t) as "Row",
        row_to_json(h) as "PK"
      from (
        select
          tg_table_name as "Source",
          '_Table2' as "Target",
          current_database() as "Db",
          true as "Prime"
        ) as ctx, (
          select
            id2 as "ID",
            case when
              (new."Field5" <> old."Field5") or
              (old."Field5" is null and new."Field5" is not null) or
              (new."Field5" is null and old."Field5" is not null) -- null,     not default
              then new."Field5"
              else old."Field5"
            end as "Field5",
            case when
              (new."Field6" <> old."Field6") or
              (old."Field6" is null and new."Field6" is not null) or
              (new."Field6" is null and old."Field6" is not null) -- not null, not default
              then new."Field6"
              else old."Field6"
            end as "Field6",
            case when
              (new."Field7" <> old."Field7") or
              (old."Field7" is null and new."Field7" is not null) or
              (new."Field7" is null and old."Field7" is not null) -- null,     default
              then new."Field7"
              else old."Field7"
            end as "Field7",
            case when
              (new."Field8" <> old."Field8") or
              (old."Field8" is null and new."Field8" is not null) or
              (new."Field8" is null and old."Field8" is not null) -- not null, default
              then new."Field8"
              else old."Field8"
            end as "Field8"
        ) t, (
          select
            id2 as "ID"
        ) h
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;
    return new;
  end if;

  if tg_op = 'DELETE' then
    select regexp_matches(old."ID1-ID2", '(\d+)-(\d+)') into ids;
    id1 := ids[1]::integer;
    id2 := ids[2]::integer;
    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(h) as "PK"
      from (
        select
          tg_table_name as "Source",
          '_Table1' as "Target",
          current_database() as "Db",
          true as "Prime"
      ) ctx, (
        select
          id1 as "ID"
      ) h
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;

    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(h) as "PK"
      from (
        select
          tg_table_name as "Source",
          '_Table2' as "Target",
          current_database() as "Db",
          true as "Prime"
      ) ctx, (
        select
          id2 as "ID"
      ) h
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;
    return old;
  end if;

  return null;
end;
$$;

drop trigger if exists "Action_process_table1_table2" on "Table1-Table2" cascade;
create trigger "Action_process_table1_table2"
  instead of insert or update or delete on "Table1-Table2"
  for each row
  execute procedure action_process_table1_table2();
