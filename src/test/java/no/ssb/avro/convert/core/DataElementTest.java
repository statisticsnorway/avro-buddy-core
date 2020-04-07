package no.ssb.avro.convert.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DataElementTest {

    private final DataElement dataElement = DataElementBuilder.root("root")
            .addValue("name", "Rune")
            .addValue("sex", "Male")
            .addChild(DataElementBuilder
                    .root("address")
                    .addValue("city", "Oslo")
                    .addValue("postcode", "0657")
                    .addChild(DataElementBuilder
                            .root("geolocation")
                            .addValue("x", "10")
                            .addValue("y", "10").build()
                    ).build()
            )
            .build();

    @Test
    void testValueInterceptor() {
        DataElement aFieldWithInterceptor = new DataElement("aFieldWithInterceptor").withValueInterceptor(
                (field, value) -> "shouldBeBlankedOut".equals(value) ? "#####" : value
        );
        aFieldWithInterceptor.setValue("shouldBeBlankedOut");
        assertThat(aFieldWithInterceptor.getValue()).isEqualTo("#####");

        DataElement aFieldWithoutInterceptor = new DataElement("aFieldWithoutInterceptor");
        aFieldWithoutInterceptor.setValue("shouldBeLeftAsIs");
        assertThat(aFieldWithoutInterceptor.getValue()).isEqualTo("shouldBeLeftAsIs");
    }

    @Test
    void testDataElementPath() {
        DataElement e = dataElement;
        assertThat(e.getPath()).isEqualTo("/root");

        e = e.findChildByName("address");
        assertThat(e.getPath()).isEqualTo("/root/address");

        e = e.findChildByName("geolocation");
        assertThat(e.getPath()).isEqualTo("/root/address/geolocation");

        e = e.findChildByName("x");
        assertThat(e.getPath()).isEqualTo("/root/address/geolocation/x");
    }

    @Test
    void testToString() {
        String expected =
                "root value:null\n" +
                        " |-- name value:Rune\n" +
                        " |-- sex value:Male\n" +
                        " |-- address value:null\n" +
                        " |    |-- city value:Oslo\n" +
                        " |    |-- postcode value:0657\n" +
                        " |    |-- geolocation value:null\n" +
                        " |    |    |-- x value:10\n" +
                        " |    |    |-- y value:10\n";

        assertThat(dataElement.toString(true)).isEqualTo(expected);

        DataElement address = dataElement.findChildByName("address");
        assertThat(address.name).isEqualTo("address");
    }

    @Test
    void testFindChildByName() {
        DataElement address = dataElement.findChildByName("address");
        assertThat(address.name).isEqualTo("address");

        DataElement city = address.findChildByName("city");
        assertThat(city.name).isEqualTo("city");
    }

    @Test
    void testChangeValue() {
        DataElement name = dataElement.findChildByName("name");
        assertThat(name.getValue()).isEqualTo("Rune");

        name.setValue("Bill");
        assertThat(name.getValue()).isEqualTo("Bill");
    }
}