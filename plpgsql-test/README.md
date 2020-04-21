
# Pruebas

Tenemos 4 bases de datos.

1. Master, el cual contiene 3 tablas, `_Table1`, `_Table2`, `_Table3` y sus respectivas vistas `Table1`, `Table2`, `Table3`.
2. La vista `Table1-Table2`
3. La vista `Table2-Table3`
4. La vista `Table1-Table2-Table3`

La vista `Table1-Table2` tiene una estructura:

"ID1-ID2", "Field1", "Field2", "Field3", "Field4", "Field5", "Field6", "Field7", "Field8"

Y la logica detras de dicha vista es modificar
- "Field1", "Field2", "Field3", "Field4" donde "ID"=ID1 afectando directamente _Table1
- "Field5", "Field6", "Field7", "Field8" donde "ID"=ID2 afectando directamente _Table2

La vista `Table2-Table3` tiene una estructura:

"ID1-ID2", "Field5", "Field6", "Field7", "Field8", "Field9", "Field10", "Field11", "Field12"

Y la logica detras de dicha vista es modificar
- "Field5", "Field6", "Field7", "Field8" donde "ID"=ID1 afectando directamente _Table2
- "Field9", "Field10", "Field11", "Field12" donde "ID"=ID2 afectando directamente _Table3

La vista `Tabla1-Table2-Table3` tiene una estructura:

"ID1-ID2-ID3", "Field1", "Field2", "Field3", "Field4", "Field5", "Field6", "Field7", "Field8", "Field9", "Field10", "Field11", "Field12"

Y la logica detras de dicha vista es modificar
- "Field1", "Field2", "Field3", "Field4" donde "ID"=ID1 afectando directamente _Table1
- "Field5", "Field6", "Field7", "Field8", "Field9", "Field10", "Field11", "Field12" donde "ID"="ID2-ID3" afectando a la vista "Table2-Table3"
