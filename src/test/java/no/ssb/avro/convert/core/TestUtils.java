package no.ssb.avro.convert.core;

import org.apache.avro.Schema;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

class TestUtils {

    private static ClassLoader classloader = Thread.currentThread().getContextClassLoader();

    private static Schema loadSchema(String fileName) {
        try {
            return new Schema.Parser().parse(getInputStream(fileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static InputStream getInputStream(String fileName) throws FileNotFoundException {
        InputStream stream = classloader.getResourceAsStream(fileName);
        if (stream == null) {
            throw new FileNotFoundException(fileName);
        }
        return stream;
    }

    static Schema avroSchemaExtended() {
        return loadSchema("test-data/avro-schema-extended.avsc");
    }

}

