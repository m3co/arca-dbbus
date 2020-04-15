
create or replace function send_jsonrpc(request json)
  returns void
  language 'plpgsql' volatile
as $$
begin
  perform pg_notify('jsonrpc', request::text);
end;
$$;

create table if not exists "_Table"
(
  "ID" serial,
  "Field1" character varying(255),
  "Field2" character varying(255) not null,
  "Field3" character varying(255) default 'default field3',
  "Field4" character varying(255) not null default 'default field4',
  constraint "Table_pkey" primary key ("ID")
)
with (
  OIDS=false
);

create or replace function notify__table_table_before()
  returns trigger
  language plpgsql volatile as
$$
declare
  r record;
begin
  if tg_op = 'INSERT' then
    return new;
  elsif tg_op = 'DELETE' then
    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(t) as "Row",
        row_to_json(h) as "PK"
      from (
        select
          tg_table_name as "Source",
          'Table' as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table"
        where "ID"=old."ID"
      ) t, (
        select
          old."ID" as "ID"
      ) h
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;
    return old;
  elsif tg_op = 'UPDATE' then
    return new;
  end if;
  return null;
end;
$$;

create or replace function notify__table_table_after()
  returns trigger
  language plpgsql volatile as
$$
declare
  r record;
begin
  if tg_op = 'INSERT' then
    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(t) as "Row",
        row_to_json(h) as "PK"
      from (
        select
          tg_table_name as "Source",
          'Table' as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table"
        where "ID"=new."ID"
      ) t, (
        select
          new."ID" as "ID"
      ) h
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;
    return new;
  elsif tg_op = 'DELETE' then
    return old;
  elsif tg_op = 'UPDATE' then
    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(t) as "Row",
        row_to_json(h) as "PK"
      from (
        select
          tg_table_name as "Source",
          'Table' as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table"
        where "ID"=new."ID"
      ) t, (
        select
          old."ID" as "ID"
      ) h
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;
    return old;
  end if;
  return null;
end;
$$;

drop trigger if exists "notification__Table_to_Table_before" on "_Table" cascade;
create trigger "notification__Table_to_Table_before"
  before insert or update or delete on "_Table"
  for each row
  execute procedure notify__table_table_before();

drop trigger if exists "notification__Table_to_Table_after" on "_Table" cascade;
create trigger "notification__Table_to_Table_after"
  after insert or update or delete on "_Table"
  for each row
  execute procedure notify__table_table_after();

create or replace view "Table"("ID", "Field1", "Field2", "Field3", "Field4") as (
  select
    "ID",
    "Field1",
    "Field2",
    "Field3",
    "Field4"
  from "_Table"
);

create or replace function action_process_table()
  returns trigger
  language 'plpgsql' volatile
as $$
declare
  r record;
begin
  -- Construcciones genericas para INSERT, DELETE, UPDATE
  if tg_op = 'INSERT' then
    if new."ID" is null then
      new."ID" = nextval('"_Table_ID_seq"');
    end if;
    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(t) as "Row"
      from (
        select
          tg_table_name as "Source",
          '_' || tg_table_name as "Target",
          current_database() as "Db",
          true as "Prime"
        ) as ctx, (
          select
            new."ID" as "ID",
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
    return new;
  end if;

  if tg_op = 'UPDATE' then
    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(t) as "Row",
        row_to_json(h) as "PK"
      from (
        select
          tg_table_name as "Source",
          '_' || tg_table_name as "Target",
          current_database() as "Db",
          true as "Prime"
        ) as ctx, (
          select
            new."ID" as "ID",
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
            old."ID" as "ID"
        ) h
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;
    return new;
  end if;

  if tg_op = 'DELETE' then
    for r in (
      select
        row_to_json(ctx) as "Context",
        lower(tg_op) as "Method",
        row_to_json(h) as "PK"
      from (
        select
          tg_table_name as "Source",
          '_' || tg_table_name as "Target",
          current_database() as "Db",
          true as "Prime"
      ) ctx, (
        select
          old."ID" as "ID"
      ) h
    ) loop
      perform send_jsonrpc(row_to_json(r));
    end loop;
    return old;
  end if;

  return null;
end;
$$;

drop trigger if exists "Action_process_table" on "Table" cascade;
create trigger "Action_process_table"
  instead of insert or update or delete on "Table"
  for each row
  execute procedure action_process_table();
