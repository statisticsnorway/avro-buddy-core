package no.ssb.avro.convert.core;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaBuddyTest {

    @Test
    void testFindRecursively() {
        Schema schema = SchemaBuilder
                .record("root").namespace("no.ssb.dataset")
                .fields()
                .name("a").type(
                        SchemaBuilder.record("a")
                                .fields()
                                .name("countryCode").type().optional().stringType()
                                .endRecord()
                ).noDefault()
                .name("b").type(
                        SchemaBuilder.record("b")
                                .fields()
                                .name("countryCode").type().optional().stringType()
                                .endRecord()
                ).noDefault()
                .endRecord();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);

        List<SchemaBuddy> countryCodes = schemaBuddy.getChildrenRecursive("countryCode");
        assertThat(countryCodes).hasSize(2);
        assertThat(countryCodes.get(0).getParent().getName()).isEqualTo("a");
        assertThat(countryCodes.get(1).getParent().getName()).isEqualTo("b");
    }

    @Test
    void checkOptionalAndNullable() {
        Schema schema = SchemaBuilder
                .record("root")
                .fields()
                .name("mandatory").type().stringType().noDefault()
                .name("optional").type().optional().stringType()
                .name("nullable").type().nullable().stringType().noDefault()
                .endRecord();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);
        String result = schemaBuddy.toString(true);
        String expected =
                "root: record optional:false nullable:false\n" +
                        " |-- mandatory: string optional:false nullable:false\n" +
                        " |-- optional: string optional:true nullable:false\n" +
                        " |-- nullable: string optional:false nullable:true\n";

        assertThat(result).isEqualTo(expected);

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
        String result = schemaBuddy.toString(true);
        String expected =
                "root: record optional:false nullable:false\n" +
                        " |-- id: string optional:false nullable:false\n" +
                        " |-- person: record optional:true nullable:false\n" +
                        " |    |-- name: string optional:false nullable:false\n" +
                        " |    |-- sex: string optional:true nullable:false\n";

        assertThat(result).isEqualTo(expected);
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

        List<String> names = schemaBuddy.getSimpleTypeChildren().stream().map(SchemaBuddy::getName).collect(Collectors.toList());
        assertThat(names).isEqualTo(Collections.singletonList("id"));

        String result = schemaBuddy.toString(true);
        String expected =
                "root: record optional:false nullable:false\n" +
                        " |-- id: string optional:false nullable:false\n" +
                        " |-- person: record optional:false nullable:false\n" +
                        " |    |-- name: string optional:false nullable:false\n" +
                        " |    |-- sex: string optional:true nullable:false\n";

        assertThat(result).isEqualTo(expected);
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
        String result = schemaBuddy.toString(true);
        String expected =
                "root: record optional:false nullable:false\n" +
                        " |-- id: string optional:false nullable:false\n" +
                        " |-- person: array optional:true nullable:false\n" +
                        " |    |-- person: record optional:true nullable:false\n" +
                        " |    |    |-- name: string optional:false nullable:false\n" +
                        " |    |    |-- sex: string optional:true nullable:false\n";

        assertThat(result).isEqualTo(expected);
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
        String result = schemaBuddy.toString(true);
        String expected =
                "root: record optional:false nullable:false\n" +
                        " |-- id: string optional:false nullable:false\n" +
                        " |-- person: array optional:false nullable:false\n" +
                        " |    |-- person: record optional:false nullable:false\n" +
                        " |    |    |-- name: string optional:false nullable:false\n" +
                        " |    |    |-- sex: string optional:true nullable:false\n";

        assertThat(result).isEqualTo(expected);
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
        String result = schemaBuddy.toString(true);
        String expected =
                "root: record optional:false nullable:false\n" +
                        " |-- id: string optional:false nullable:false\n" +
                        " |-- person: array optional:true nullable:false\n" +
                        " |    |-- person: string optional:true nullable:false\n";

        assertThat(result).isEqualTo(expected);
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
        String result = schemaBuddy.toString(true);
        String expected =
                "root: record optional:false nullable:false\n" +
                        " |-- id: string optional:false nullable:false\n" +
                        " |-- person: array optional:false nullable:false\n" +
                        " |    |-- person: string optional:false nullable:false\n";

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void checkCallback() {
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
                .name("languages").type(
                        SchemaBuilder.array()
                                .items().stringType()
                ).noDefault()
                .endRecord();

        StringBuilder sb = new StringBuilder();
        SchemaBuddy.parse(schema, schemaBuddy -> {
            if (schemaBuddy.isBranch()) {
                sb.append("branch  : ").append(schemaBuddy.toLevelString()).append("\n");
            }
            if (schemaBuddy.isSimpleType()) {
                sb.append("instance: ").append(schemaBuddy.toLevelString()).append("\n");
            }
        });

        String expected = "" +
                "branch  : root: record optional:false nullable:false\n" +
                "instance:  |-- id: string optional:false nullable:false\n" +
                "branch  :  |    |-- person: record optional:false nullable:false\n" +
                "instance:  |    |    |-- name: string optional:false nullable:false\n" +
                "instance:  |    |    |-- sex: string optional:true nullable:false\n" +
                // TODO: check of we handle array of simple elements in GSIM/LDS
                //"branch  :  |-- languages: array optional:false nullable:false\n" +
                "instance:  |    |-- languages: string optional:false nullable:false\n";

        assertThat(sb.toString()).isEqualTo(expected);
    }

    @Test
    void extraFieldsInAvroJson() {
        Schema schema = TestUtils.avroSchemaExtended();

        StringBuilder sb = new StringBuilder();
        SchemaBuddy.parse(schema, schemaBuddy -> {
            if (schemaBuddy.isBranch()) {
                sb.append("branch  : ").append(schemaBuddy.toLevelString()).append("\n");
                sb.append("          ").append(schemaBuddy.getIntendString())
                        .append(schemaBuddy.getProp("extra")).append("\n");
            }
            if (schemaBuddy.isSimpleType()) {
                sb.append("instance: ").append(schemaBuddy.toLevelString()).append("\n");
                sb.append("          ").append(schemaBuddy.getIntendString())
                        .append(schemaBuddy.getProp("extra")).append("\n");
            }
        });

        String expected = "" +
                "branch  : root: record optional:false nullable:false\n" +
                "          record-root\n" +
                "instance:  |-- id: string optional:false nullable:false\n" +
                "           |-- fields-string-id\n" +
                "branch  :  |    |-- person: record optional:false nullable:false\n" +
                "           |    |-- array-extra\n" +
                "instance:  |    |    |-- name: string optional:false nullable:false\n" +
                "           |    |    |-- person-name-extra\n" +
                "instance:  |    |    |-- sex: string optional:true nullable:false\n" +
                "           |    |    |-- person-sex-extra\n" +
                // TODO: check of we handle array of simple elements in GSIM/LDS
                //"branch  :  |-- languages: array optional:false nullable:false\n" +
                //"           |-- languages-with-child-type-array\n" +
                "instance:  |    |-- languages: string optional:false nullable:false\n" +
                "           |    |-- array with items string\n";

        assertThat(sb.toString()).isEqualTo(expected);
    }

    @Test
    void testCallBack() {
        Schema schema = TestUtils.avroSchemaExtended();

        SchemaBuddy.parse(schema, schemaBuddy -> {
            if (schemaBuddy.isRoot()) {
                assertThat(schemaBuddy.getName()).isEqualTo("root");
            }
        });
    }

    @Test
    void testToStringEnricher() {
        Schema schema = TestUtils.avroSchemaExtended();
        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);
        String output = schemaBuddy.toString(true, item ->
          String.format("%s: %s optional:%s nullable:%s %s",
            item.getName(),
            item.getType().getName(),
            item.isOptional(),
            item.isNullable(),
            item.getName().toUpperCase()
            ));

        String expected = "" +
          "root: record optional:false nullable:false ROOT\n" +
          " |-- id: string optional:false nullable:false ID\n" +
          " |-- person: array optional:false nullable:false PERSON\n" +
          " |    |-- person: record optional:false nullable:false PERSON\n" +
          " |    |    |-- name: string optional:false nullable:false NAME\n" +
          " |    |    |-- sex: string optional:true nullable:false SEX\n" +
          " |-- languages: array optional:false nullable:false LANGUAGES\n" +
          " |    |-- languages: string optional:false nullable:false LANGUAGES\n";

        assertThat(output).isEqualTo(expected);
    }
}

