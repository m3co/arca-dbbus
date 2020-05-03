
# arca-DB-bus

Aqui tenemos una herramienta interna que permite hacer `SIDUS`. _SIDUS_ son las iniciales de las acciones _Select_, _Insert_, _Delete_, _Update_, _Search_. Es altamente probable que ya existan cientos de bibliotecas que administran dichas acciones. Pero, teniendo en cuenta que es Arca, entonces...

## Estructura de las acciones

### Request

El _request_ se conforma de los siguientes campos (con valores de ejemplo):

```ts
interface Request {
    ID    : uuid4();
    Method: "Select" | "Insert" | "Delete" | "Update" | "Search";
    Params: {
        PK?: {
            ID : Number;
            Key: String | null;
            ...
        };
        Row?: {
            Key        : String | null;
            Description: String | null;
            ...
        };
        Limit?: Number;
        Offset?: Number;
    }
}
```

`SIDUS` utiliza solamente `PK` y `Row` en conjunto para procesar la accion correspondiente. Al analizar los casos particulares, se restalta que:

- `Select` es opcional `PK`
- `Insert` solo requiere de `Row`
- `Delete` solo requiere de `PK`
- `Update` requiere de `Row` y de `PK`
- `Search` requiere de `PK`

### Responses

Un _Request_ de _Insert_, _Delete_ o de _Update_ retorna entonces

```ts
interface Response {
    ID    : uuid4();
    Method: "Insert" | "Delete" | "Update";
    Result: {
        Success: true;
        PK: {
            ID : Number;
            Key: String | null;
            ...
        };
    };
}
```

Si no se indica el parametro _PK_ entonces se ejecuta la _query_ retornando

```ts
interface Response {
    ID    : uuid4();
    Method: "Insert" | "Delete" | "Update";
    Result: {
        Success: true;
    };
}
```

Nótese la diferencia de ésta respuesta contra la respuesta anterior. `PK` no está indicado. (Éso aún está por revisar).

## Los tipos permitidos

Resulta que en PostgreSQL existen tipos de datos correctamente definidos.
[Tipos](https://www.postgresql.org/docs/10/datatype.html).

En especial, vamos a utilizar [Numericos](https://www.postgresql.org/docs/10/datatype-numeric.html), [Caracteres](https://www.postgresql.org/docs/10/datatype-character.html), [Fechas](https://www.postgresql.org/docs/10/datatype-datetime.html), [Boolean](https://www.postgresql.org/docs/10/datatype-boolean.html), [Enums](https://www.postgresql.org/docs/10/datatype-enum.html).

Es posible implementar en DB-BUS el procesamiento de todos los tipos existentes en PostgreSQL. Mas dicho objetivo no es necesario puesto que ARCA utiliza un juego de datos bastante limitado, el cual es:

- [Character Varying, Text](https://www.postgresql.org/docs/10/datatype-character.html)
- [Integer](https://www.postgresql.org/docs/10/datatype-numeric.html#DATATYPE-INT)
- [Numeric](https://www.postgresql.org/docs/10/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL)
- [Boolean](https://www.postgresql.org/docs/10/datatype-boolean.html)
- [Date, Timestamp](https://www.postgresql.org/docs/10/datatype-datetime.html#DATATYPE-DATETIME-INPUT)
- [Enums](https://www.postgresql.org/docs/10/datatype-enum.html)

Por lo tanto, DB-BUS debe exponer los datos que está en capacidad de procesar a los entes interesados. En éste cáso sería exponerle ésa lista a Arca-Server.

## PK

Éste parámetro se torna en la clausula `where`. `PK` es un objeto que transforma su contenido en una expresion booleana utilizando sólamente dos operadores: `and` y `or`.

Ejemplo 1:

```ts
// PK =
{
    "Field1": "Value1",
    "Field2": "Value2",
}
```

Su resultado sería:
`where "Field1"="Value1" and "Field2"="Value2"`.


Ejemplo 2:

```ts
// PK =
{
    "Field1": ["ValueA", "ValueB"],
    "Field2": "Value2",
}
```

Su resultado sería:
`where ("Field1"="ValueA" or "Field1"="ValueB") and "Field2"="Value2"`.


Para los booleanos y nulls, tenémos,
Ejemplo 3:

```ts
// PK =
{
    "Field1": true,
    "Field2": null,
}
```

Su resultado sería:
`where "Field1" is true and "Field2" is null`.
