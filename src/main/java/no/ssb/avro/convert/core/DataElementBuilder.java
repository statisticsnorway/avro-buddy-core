package no.ssb.avro.convert.core;


class DataElementBuilder {

    private DataElementBuilder() {
    }

    static class RootBuilder {
        private DataElement dataElement;

        RootBuilder(DataElement dataElement) {
            this.dataElement = dataElement;
        }

        RootBuilder addValue(String name, String value) {
            dataElement.addChild(new DataElement(name, value));
            return this;
        }

        RootBuilder addChild(DataElement dataElement) {
            this.dataElement.addChild(dataElement);
            return this;
        }

        DataElement build() {
            return dataElement;
        }
    }

    static RootBuilder root(String name) {
        return new RootBuilder(new DataElement(name));
    }
}