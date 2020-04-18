
/*
  Definamos la documentacion sobre el estilo de codigo en pl/pgSQL
*/

/*
  send_jsonrpc - es para enviar por el canal jsonrpc la informacion a go
*/
create or replace function send_jsonrpc(request json)
  returns void
  language 'plpgsql' volatile
as $$
begin
  perform pg_notify('jsonrpc', request::text);
end;
$$;

/*
  "_Table" es un ejemplo de una tabla primaria
  Nótese que tiene un _ al principio.
  Una tabla primaria NO debe ser modificada por ningún cliente directamente.
  Cualquier interación a realizar sobre una tabla primaria debe pasar
  por su vista. Una vista es el reflejo de una tabla primaria y debe
  ser nombrada con el mismo nombre de la tabla primaria sin anteponerle _.
  Es decir, "_Table" - es primaria y "Table" es la vista. Ver más abajo.
*/
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

/*
  notify__table_table_before tiene una forma sencilla de formar el nombre
  _      - es el caractér para separar
  notify - indica que la función es para realizar una notificación
  _table - indica la fuente de la notificacion
  table  - indica el destino de la notificación
  before - indica la propiedad temporal impuesta por la naturaleza de los triggers

  A manera de ejemplo, podriamos tener un nombre como
  notify__taable_viewcomplextable_before y éste caso indica que una modificacion
  realizada en _table dispara una notificación acerca que debe verse reflejado en
  la vista viewcomplextable

  Si viewcomplextable depende de _table entonces una modificación sobre _table
  afecta una o más entradas en viewcomplextable. Los cambios en ésas entradas
  deben ser notificados a las partes interesadas.
*/
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
          substring(tg_table_name, '^_(.*)') as "Target",
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

/*
  notify__table_table_after la misma historia que el caso anterior, solo que
  la diferencia radica en su propiedad temporal
*/
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
          substring(tg_table_name, '^_(.*)') as "Target",
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
          substring(tg_table_name, '^_(.*)') as "Target",
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

/*
 "Table" es un ejemplo de una vista sobre una tabla primaria
 Nótese que el nombre de la vista es cási idéntico al nombre de la tabla primaria.
 Medíante ésta vista es que se realizan los cambios sobre la tabla primaria, cambios
 provenientes por parte de los clientes.
 Una tabla primaria NO debe exponerse a los clientes finales.
 La razón de ésta reestricción es que si un cambio cae directamente sobre una
 tabla primaria entonces dicho cambio NO se va a propagar dentro del cluster.
*/
create or replace view "Table"("ID", "Field1", "Field2", "Field3", "Field4") as (
  select
    "ID",
    "Field1",
    "Field2",
    "Field3",
    "Field4"
  from "_Table"
);

/*
  action_process_table tiene una forma sencilla de formar el nombre
  _       - es el caractér para separar
  action  - indica que la función es para realizar procesar una accion
  process - deberia borrarlo...
  table   - indica la fuente(necesariamente una vista) de la accion a procesar
*/
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

/*
  En resumen, tenemos

  Una tabla primaria ("Source") se nombra con un prefijo: "_Source"
  Una tabla primaria debe tener sus notificaciones mediante
  "notify_[_source]_[target]_[before/after]"

  La vista sobre "Source" se nombra sin prefijo: "Source"
  La vista sobre "Source" debe procesar las acciones IDU mediante
  "action_process_[source]"

  A modo de sugerencia,
  - notify podria simplificarse con el mnemonico ntf_[_source]_[target]_[b/a]
  - action podria simplificarse con el mnemonico act_[source]
*/
