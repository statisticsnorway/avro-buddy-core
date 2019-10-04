# Avro buddy core

Lib for converting in data to GenericRecord which can be used to create Avro/Parquet files  

## Usage

```java
// Create DataElement 
DataElement dataElement = DataElementBuilder.root("root")
        .addValue("mandatory", "v1")
        .addValue("optional", "v2")
        .addValue("nullable", null).build();

// Create avro schema  
Schema schema = SchemaBuilder
        .record("root")
        .fields()
        .name("mandatory").type().stringType().noDefault()
        .name("optional").type().optional().stringType()
        .name("nullable").type().nullable().stringType().noDefault()
        .endRecord();

// Create SchemaBuddy  
SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);

// Create GenericRecord
GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);

```
record.toString() gives =

```json
{
  "mandatory": "v1",
  "optional": "v2",
  "nullable": "null"
}
```
