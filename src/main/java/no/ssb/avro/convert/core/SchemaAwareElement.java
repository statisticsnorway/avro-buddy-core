package no.ssb.avro.convert.core;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SchemaAwareElement us used to link {@link no.ssb.avro.convert.core.DataElement} to Avro schema.
 * Can then use toRecord to create {@link org.apache.avro.generic.GenericRecord}
 */
public class SchemaAwareElement {

    private final String name;
    private final SchemaAwareElement parent;
    private final String value;
    private final List<SchemaAwareElement> children = new ArrayList<>();
    private final SchemaBuddy schemaBuddy;

    public SchemaAwareElement(String name, String value, SchemaAwareElement parent, SchemaBuddy schemaBuddy) {
        this.name = name;
        this.parent = parent;
        this.value = value;
        this.schemaBuddy = schemaBuddy;
    }

    public static SchemaAwareElement toSchemaAwareElement(DataElement dataElement, SchemaBuddy schemaBuddy) {
        return toSchemaAwareElement(dataElement, null, schemaBuddy);
    }

    public static GenericRecord toRecord(DataElement dataElement, SchemaBuddy schemaBuddy) {
        return toSchemaAwareElement(dataElement, schemaBuddy).toRecord();
    }

    public GenericRecord toRecord() {
        return toRecord(schemaBuddy);
    }

    private void addChild(SchemaAwareElement child) {
        children.add(child);
    }

    private boolean isSimpleType() {
        return !schemaBuddy.isArrayType() && schemaBuddy.getType() != Schema.Type.RECORD;
    }

    private boolean isArrayType() {
        return schemaBuddy.isArrayType();
    }

    private Schema.Type getArrayType() {
        if (!isArrayType()) {
            throw new IllegalStateException("Can only get getArrayType when we have type ARRAY. Was:" + schemaBuddy.getType());
        }
        return schemaBuddy.findChildren().get(0).getType();
    }

    private List<SchemaAwareElement> getChildren() {
        return children;
    }

    private static SchemaAwareElement toSchemaAwareElement(DataElement dataElement, SchemaAwareElement parent, SchemaBuddy schemaBuddy) {
        SchemaAwareElement schemaAwareElement = new SchemaAwareElement(dataElement.name, dataElement.getValue(), parent, schemaBuddy);

        Map<String, List<DataElement>> arrayTypes = recursivelyMapToSchemaAwareElementAndCollectArrayTypes(dataElement, schemaBuddy, schemaAwareElement);

        arrayTypes.forEach((name, subElements) -> {
            SchemaBuddy arrayTypeSchema = schemaBuddy.getChild(name);
            recursivelyMapArrayElementsToSchemaAwareElement(arrayTypeSchema, schemaAwareElement, name, subElements);
        });

        return schemaAwareElement;
    }

    private static Map<String, List<DataElement>> recursivelyMapToSchemaAwareElementAndCollectArrayTypes(DataElement dataElement, SchemaBuddy schemaBuddy, SchemaAwareElement schemaAwareElement) {
        Map<String, List<DataElement>> nameToList = new HashMap<>();
        for (DataElement child : dataElement.getChildren()) {
            SchemaBuddy schemaBuddyChild = schemaBuddy.getChild(child.getName());
            if (schemaBuddyChild.isArrayType()) {
                // For a datasource like xml we can have multiple elements with same name
                // In the avro schema this will be array type and we need to collect all elements in a list
                List<DataElement> dataElements = nameToList.getOrDefault(child.getName(), new ArrayList<>());
                dataElements.add(child);
                nameToList.putIfAbsent(child.getName(), dataElements);
            } else {
                if (schemaBuddy.isSimpleType()) {
                    schemaAwareElement.addChild(new SchemaAwareElement(child.getName(), child.getValue(), schemaAwareElement, schemaBuddyChild));
                } else {
                    schemaAwareElement.addChild(toSchemaAwareElement(child, schemaAwareElement, schemaBuddyChild));
                }
            }
        }
        return nameToList;
    }

    private static void recursivelyMapArrayElementsToSchemaAwareElement(SchemaBuddy arrayTypeSchema, SchemaAwareElement schemaAwareElement, String name, List<DataElement> subElements) {
        SchemaAwareElement arraySchemaAwareElement = new SchemaAwareElement(name, null, schemaAwareElement, arrayTypeSchema);

        // If we don't have a value, it's a record
        if (subElements.get(0).getValue() == null) {
            for (DataElement child : subElements) {
                arraySchemaAwareElement.addChild(toSchemaAwareElement(child, arraySchemaAwareElement, arrayTypeSchema.getArrayTypeSchema()));
            }
        } else {
            for (DataElement child : subElements) {
                arraySchemaAwareElement.addChild(new SchemaAwareElement(child.getName(), child.getValue(), arraySchemaAwareElement, arrayTypeSchema.getArrayTypeSchema()));
            }
        }
        schemaAwareElement.addChild(arraySchemaAwareElement);
    }

    public String toString(boolean recursive) {
        StringBuilder sb = new StringBuilder();
        if (recursive) {
            sb.append(String.format("%s%s value:%s schema(%s)%n", getIndentString(), name, value, schemaBuddy));

            for (SchemaAwareElement child : getChildren()) {
                sb.append(child.toString(true));
            }
        } else {
            sb.append(String.format("%s value:%s schema(%s)", name, value, schemaBuddy));
        }

        return sb.toString();
    }

    private GenericRecord toRecord(SchemaBuddy schemaBuddy) {
        GenericRecordBuilder rootRecordBuilder = new GenericRecordBuilder(schemaBuddy.getSchema());
        return toRecord(rootRecordBuilder);
    }

    private GenericRecord toRecord(GenericRecordBuilder rootRecordBuilder) {
        if (value != null) {
            setSimpleType(rootRecordBuilder, schemaBuddy.getType(), this);
            return rootRecordBuilder.build();
        }

        for (SchemaAwareElement child : getChildren()) {
            if (child.isArrayType()) {
                Schema.Type arrayType = child.getArrayType();
                if (arrayType == Schema.Type.RECORD) {
                    SchemaBuddy arrayTypeSchema = child.schemaBuddy.getArrayTypeSchema();
                    List<GenericRecord> records = child.getChildren()
                            .stream()
                            .map(subElement -> subElement.toRecord(arrayTypeSchema))
                            .collect(Collectors.toList());
                    rootRecordBuilder.set(child.name, records);
                } else {
                    List<SchemaAwareElement> childrenAsschemaAwareElements = child.getChildren();
                    rootRecordBuilder.set(
                            child.name,
                            childrenAsschemaAwareElements.stream()
                                    // TODO: make this convert value based on type so we support more than String
                                    .map(subElement -> subElement.value)
                                    .collect(Collectors.toList()));
                }
            } else {
                if (child.isSimpleType()) {
                    setSimpleType(rootRecordBuilder, child.schemaBuddy.getType(), child);
                } else {
                    GenericRecord record = child.toRecord();
                    rootRecordBuilder.set(child.name, record);
                }
            }
        }
        return rootRecordBuilder.build();
    }

    private void setSimpleType(GenericRecordBuilder rootRecordBuilder, Schema.Type type, SchemaAwareElement element) {
        try {
            switch (type) {
                case LONG:
                    rootRecordBuilder.set(element.name, Long.parseLong(element.value));
                    break;
                case INT:
                    rootRecordBuilder.set(element.name, Integer.parseInt(element.value));
                    break;
                case DOUBLE:
                    rootRecordBuilder.set(element.name, Double.parseDouble(element.value));
                    break;
                case BOOLEAN:
                    rootRecordBuilder.set(element.name, Boolean.parseBoolean(element.value));
                    break;
                case MAP:
                    // TODO: make tests for this and implement correctly
                    // Collections.singletonMap("a", "b")
                    rootRecordBuilder.set(element.name, new HashMap<>());
                    break;
                case STRING:
                    if (!element.schemaBuddy.isOptional() && element.value == null) {
                        // We have a case where avro schema requires a value
                        // But data source don't send data
                        // For now we are adding a default value for this
                        // TODO: add an argument which decides if we should add default value or throw exception
                        rootRecordBuilder.set(element.name, "null");
                    } else {
                        rootRecordBuilder.set(element.name, element.value);
                    }
                    break;
                default:
                    throw new IllegalStateException(type + " do not currently have a converter");
            }
        } catch (RuntimeException e) {
            throw new RuntimeException(e.getMessage() + String.format("%ndata:(%s)", element.toString()), e);
        }
    }

    private String getIndentString() {
        int size = getParents().size();
        if (size == 0) return "";
        if (size == 1) return " |-- ";
        return String.join("", Collections.nCopies(size - 1, " |   ")) + " |-- ";
    }

    @Override
    public String toString() {
        return toString(false);
    }

    private List<String> getParents() {
        SchemaAwareElement currentParent = parent;
        List<String> parentList = new ArrayList<>();
        while (currentParent != null) {
            parentList.add(currentParent.name);
            currentParent = currentParent.parent;
        }
        return parentList;
    }

}
