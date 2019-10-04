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
}