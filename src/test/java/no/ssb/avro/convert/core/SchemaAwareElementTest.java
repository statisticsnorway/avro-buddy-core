package no.ssb.avro.convert.core;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaAwareElementTest {

    @Test
    void checkOptionalAndNullableSchema() {
        {
            Schema schema = SchemaBuilder
                    .record("root")
                    .fields()
                    .name("mandatory").type().stringType().noDefault()
                    .name("optional").type().optional().stringType()
                    .name("nullable").type().nullable().stringType().noDefault()
                    .endRecord();

            SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);

            DataElement dataElement = DataElementBuilder.root("root")
                    .addValue("mandatory", "v1")
                    .addValue("optional", "v2")
                    .addValue("nullable", null).build();

            GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            String json = gson.toJson(new JsonParser().parse(record.toString()));

            String expectedRecords = "{\n" +
                    "  \"mandatory\": \"v1\",\n" +
                    "  \"optional\": \"v2\",\n" +
                    "  \"nullable\": \"null\"\n" +
                    "}";

            assertThat(json).isEqualTo(expectedRecords);
        }
    }

    @Test
    void checkOptionalChild() {
        Schema schema = SchemaBuilder
                .record("root").namespace("no.ssb.dataset")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("person").type().optional().type(
                        SchemaBuilder.record("person")
                                .fields()
                                .name("name").type().stringType().noDefault()
                                .name("sex").type().optional().stringType()
                                .endRecord()
                )
                .endRecord();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);

        DataElement dataElement = DataElementBuilder.root("root")
                .addValue("id", "007")
                .addChild(DataElementBuilder
                        .root("person")
                        .addValue("name", "James Bond")
                        .addValue("sex", "Male").build())
                .build();
        GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(new JsonParser().parse(record.toString()));

        String expectedRecords = "{\n" +
                "  \"id\": \"007\",\n" +
                "  \"person\": {\n" +
                "    \"name\": \"James Bond\",\n" +
                "    \"sex\": \"Male\"\n" +
                "  }\n" +
                "}";

        assertThat(json).isEqualTo(expectedRecords);
    }

    @Test
    void checkMandatoryChild() {
        Schema schema = SchemaBuilder
                .record("root").namespace("no.ssb.dataset")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("person").type(
                        SchemaBuilder.record("person")
                                .fields()
                                .name("name").type().stringType().noDefault()
                                .name("sex").type().optional().stringType()
                                .endRecord()
                ).noDefault()
                .endRecord();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);

        DataElement dataElement = DataElementBuilder.root("root")
                .addValue("id", "007")
                .addChild(DataElementBuilder
                        .root("person")
                        .addValue("name", "James Bond")
                        .addValue("sex", "Male").build())
                .build();
        GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(new JsonParser().parse(record.toString()));

        String expectedRecords = "{\n" +
                "  \"id\": \"007\",\n" +
                "  \"person\": {\n" +
                "    \"name\": \"James Bond\",\n" +
                "    \"sex\": \"Male\"\n" +
                "  }\n" +
                "}";

        assertThat(json).isEqualTo(expectedRecords);

    }

    @Test
    void checkArrayOptionalOfRecords() {
        Schema schema = SchemaBuilder
                .record("root").namespace("no.ssb.dataset")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("person").type().optional().type(
                        SchemaBuilder.array()
                                .items(SchemaBuilder.record("person")
                                        .fields()
                                        .name("name").type().stringType().noDefault()
                                        .name("sex").type().optional().stringType()
                                        .endRecord()
                                )
                )
                .endRecord();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);

        DataElement dataElement = DataElementBuilder.root("root")
                .addValue("id", "007")
                .addChild(DataElementBuilder
                        .root("person")
                        .addValue("name", "James Bond")
                        .addValue("sex", "Male").build())
                .build();
        GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(new JsonParser().parse(record.toString()));

        String expectedRecords = "{\n" +
                "  \"id\": \"007\",\n" +
                "  \"person\": [\n" +
                "    {\n" +
                "      \"name\": \"James Bond\",\n" +
                "      \"sex\": \"Male\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        assertThat(json).isEqualTo(expectedRecords);
    }

    @Test
    void checkArrayMandatoryOfRecords() {
        Schema schema = SchemaBuilder
                .record("root").namespace("no.ssb.dataset")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("person").type(
                        SchemaBuilder.array()
                                .items(SchemaBuilder.record("person")
                                        .fields()
                                        .name("name").type().stringType().noDefault()
                                        .name("sex").type().optional().stringType()
                                        .endRecord()
                                )
                ).noDefault()
                .endRecord();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);
        DataElement dataElement = DataElementBuilder.root("root")
                .addValue("id", "007")
                .addChild(DataElementBuilder
                        .root("person")
                        .addValue("name", "James Bond")
                        .addValue("sex", "Male").build())
                .build();
        GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(new JsonParser().parse(record.toString()));

        String expectedRecords = "{\n" +
                "  \"id\": \"007\",\n" +
                "  \"person\": [\n" +
                "    {\n" +
                "      \"name\": \"James Bond\",\n" +
                "      \"sex\": \"Male\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        assertThat(json).isEqualTo(expectedRecords);
    }

    @Test
    void checkOptionalArrayOfSimpleTypes() {
        Schema schema = SchemaBuilder
                .record("root").namespace("no.ssb.dataset")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("person").type().optional().type(
                        SchemaBuilder.array()
                                .items().stringType()
                )
                .endRecord();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);
        DataElement dataElement = DataElementBuilder.root("root")
                .addValue("id", "007")
                .build();
        GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(new JsonParser().parse(record.toString()));

        String expectedRecords = "{\n" +
                "  \"id\": \"007\"\n" +
                "}";

        assertThat(json).isEqualTo(expectedRecords);
    }

    @Test
    void checkMandatoryArrayOfSimpleTypes() {
        Schema schema = SchemaBuilder
                .record("root").namespace("no.ssb.dataset")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("person").type(
                        SchemaBuilder.array()
                                .items().stringType()
                ).noDefault()
                .endRecord();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);
        DataElement dataElement = DataElementBuilder.root("root")
                .addValue("id", "007")
                .addValue("person", "Bill Gates")
                .addValue("person", "Steve Jobs")
                .build();
        GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(new JsonParser().parse(record.toString()));

        String expectedRecords = "{\n" +
                "  \"id\": \"007\",\n" +
                "  \"person\": [\n" +
                "    \"Bill Gates\",\n" +
                "    \"Steve Jobs\"\n" +
                "  ]\n" +
                "}";

        assertThat(json).isEqualTo(expectedRecords);
    }
}