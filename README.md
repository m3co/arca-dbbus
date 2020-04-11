
# arca-DB-bus

Aqui tenemos una herramienta interna que permite hacer `IDUS`. _IDUS_ son las iniciales de las acciones _Insert_, _Delete_, _Update_, _Select_. Es altamente probable que ya existan cientos de bibliotecas que administran dichas acciones. Pero, teniendo en cuenta que es Arca, entonces...

## Estructura de las acciones

### Request

El _request_ se conforma de los siguientes campos (con valores de ejemplo):

```ts
interface Request {
    ID    : uuid4();
    Method: "Insert" | "Delete" | "Update" | "Select";
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
        }
    }
}
```

`IDUS` utiliza solamente `PK` y `Row` en conjunto para procesar la accion correspondiente. Al analizar los casos particulares, se restalta que:

- `Insert` solo requiere de `Row`
- `Delete` solo requiere de `PK`
- `Update` requiere de `Row` y de `PK`
- `Select` es opcional `PK`

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

Si no se indica el parametro _PK_ entonces se ejecuta la _query_ retornando entonces

```ts
interface Response {
    ID    : uuid4();
    Method: "Insert" | "Delete" | "Update";
    Result: {
        Success: true;
    };
}
```
