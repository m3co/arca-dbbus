
create or replace function send_jsonrpc(request json)
  returns void
  language 'plpgsql' volatile
as $$
begin
  perform pg_notify('jsonrpc', request::text);
end;
$$;

/*
  _Table1 <
*/

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

create or replace function notify__table1_table1_before()
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
          substring(tg_table_name, '^_(.*)') as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table1"
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

create or replace function notify__table1_table1_after()
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
          substring(tg_table_name, '^_(.*)') as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table1"
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
          substring(tg_table_name, '^_(.*)') as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table1"
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

drop trigger if exists "notification__Table1_to_Table1_before" on "_Table1" cascade;
create trigger "notification__Table1_to_Table1_before"
  before insert or update or delete on "_Table1"
  for each row
  execute procedure notify__table1_table1_before();

drop trigger if exists "notification__Table1_to_Table1_after" on "_Table1" cascade;
create trigger "notification__Table1_to_Table1_after"
  after insert or update or delete on "_Table1"
  for each row
  execute procedure notify__table1_table1_after();

create or replace view "Table1"("ID", "Field1", "Field2", "Field3", "Field4") as (
  select
    "ID",
    "Field1",
    "Field2",
    "Field3",
    "Field4"
  from "_Table1"
);

create or replace function action_process_table1()
  returns trigger
  language 'plpgsql' volatile
as $$
declare
  r record;
begin
  if tg_op = 'INSERT' then
    if new."ID" is null then
      new."ID" = nextval('"_Table1_ID_seq"');
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

drop trigger if exists "Action_process_table1" on "Table1" cascade;
create trigger "Action_process_table1"
  instead of insert or update or delete on "Table1"
  for each row
  execute procedure action_process_table1();

/*
  _Table1 >
*/


/*
  _Table2 <
*/

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

create or replace function notify__table2_table2_before()
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
          substring(tg_table_name, '^_(.*)') as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table2"
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

create or replace function notify__table2_table2_after()
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
          substring(tg_table_name, '^_(.*)') as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table2"
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
          substring(tg_table_name, '^_(.*)') as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table2"
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

drop trigger if exists "notification__Table2_to_Table2_before" on "_Table2" cascade;
create trigger "notification__Table2_to_Table2_before"
  before insert or update or delete on "_Table2"
  for each row
  execute procedure notify__table2_table2_before();

drop trigger if exists "notification__Table2_to_Table2_after" on "_Table2" cascade;
create trigger "notification__Table2_to_Table2_after"
  after insert or update or delete on "_Table2"
  for each row
  execute procedure notify__table2_table2_after();

create or replace view "Table2"("ID", "Field5", "Field6", "Field7", "Field8") as (
  select
    "ID",
    "Field5",
    "Field6",
    "Field7",
    "Field8"
  from "_Table2"
);

create or replace function action_process_table2()
  returns trigger
  language 'plpgsql' volatile
as $$
declare
  r record;
begin
  if tg_op = 'INSERT' then
    if new."ID" is null then
      new."ID" = nextval('"_Table2_ID_seq"');
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

drop trigger if exists "Action_process_table2" on "Table2" cascade;
create trigger "Action_process_table2"
  instead of insert or update or delete on "Table2"
  for each row
  execute procedure action_process_table2();

/*
  _Table2 <
*/


/*
  _Table3 <
*/

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

create or replace function notify__table3_table3_before()
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
          substring(tg_table_name, '^_(.*)') as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table3"
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

create or replace function notify__table3_table3_after()
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
          substring(tg_table_name, '^_(.*)') as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table3"
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
          substring(tg_table_name, '^_(.*)') as "Target",
          current_database() as "Db",
          true as "Notification"
      ) ctx, (
        select *
        from "Table3"
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

drop trigger if exists "notification__Table3_to_Table3_before" on "_Table3" cascade;
create trigger "notification__Table3_to_Table3_before"
  before insert or update or delete on "_Table3"
  for each row
  execute procedure notify__table3_table3_before();

drop trigger if exists "notification__Table3_to_Table3_after" on "_Table3" cascade;
create trigger "notification__Table3_to_Table3_after"
  after insert or update or delete on "_Table3"
  for each row
  execute procedure notify__table3_table3_after();

create or replace view "Table3"("ID", "Field9", "Field10", "Field11", "Field12") as (
  select
    "ID",
    "Field9",
    "Field10",
    "Field11",
    "Field12"
  from "_Table3"
);

create or replace function action_process_table3()
  returns trigger
  language 'plpgsql' volatile
as $$
declare
  r record;
begin
  if tg_op = 'INSERT' then
    if new."ID" is null then
      new."ID" = nextval('"_Table3_ID_seq"');
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
            case when new."Field9" is null -- null,     not default
              then null
              else new."Field9"
            end as "Field9",
            case when new."Field10" is null -- not null, not default
              then null
              else new."Field10"
            end as "Field10",
            case when new."Field11" is null -- null,     default
              then 'default field11' -- repeat default value
              else new."Field11"
            end as "Field11",
            case when new."Field12" is null -- not null, default
              then 'default field12' -- repreat default value
              else new."Field12"
            end as "Field12"
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
              (new."Field9" <> old."Field9") or
              (old."Field9" is null and new."Field9" is not null) or
              (new."Field9" is null and old."Field9" is not null) -- null,     not default
              then new."Field9"
              else old."Field9"
            end as "Field9",
            case when
              (new."Field10" <> old."Field10") or
              (old."Field10" is null and new."Field10" is not null) or
              (new."Field10" is null and old."Field10" is not null) -- not null, not default
              then new."Field10"
              else old."Field10"
            end as "Field10",
            case when
              (new."Field11" <> old."Field11") or
              (old."Field11" is null and new."Field11" is not null) or
              (new."Field11" is null and old."Field11" is not null) -- null,     default
              then new."Field11"
              else old."Field11"
            end as "Field11",
            case when
              (new."Field12" <> old."Field12") or
              (old."Field12" is null and new."Field12" is not null) or
              (new."Field12" is null and old."Field12" is not null) -- not null, default
              then new."Field12"
              else old."Field12"
            end as "Field12"
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

drop trigger if exists "Action_process_table3" on "Table3" cascade;
create trigger "Action_process_table3"
  instead of insert or update or delete on "Table3"
  for each row
  execute procedure action_process_table3();

/*
  _Table3 <
*/

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
