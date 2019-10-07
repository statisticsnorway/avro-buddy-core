package no.ssb.avro.convert.core;

import org.apache.avro.Schema;

import java.util.*;
import java.util.stream.Collectors;

/**
 * SchemaBuddy is a helper class for working with AvroSchema and is used for
 * mapping input data to schema and producing {@link org.apache.avro.generic.GenericRecord}
 * See {@link no.ssb.avro.convert.core.SchemaAwareElement}
 */
public class SchemaBuddy {

    private SchemaBuddy parent;

    private final List<SchemaBuddy> children = new ArrayList<>();
    private final Schema schema;
    private final String name;
    private final Map<String, Object> props;
    private final boolean optional;
    private final boolean nullable;

    private final String uuidString = UUID.randomUUID().toString();

    private SchemaBuddy(Schema schema, String name, Map<String, Object> props, boolean optional, boolean nullable) {
        this.schema = schema;
        this.name = name;
        this.props = props;
        this.optional = optional;
        this.nullable = nullable;
    }

    private SchemaBuddy(Schema schema, String name, Map<String, Object> props, boolean optional) {
        this(schema, name, props, optional, false);
    }

    public static SchemaBuddy parse(Schema schema) {
        return SchemaParser.parse(schema);
    }

    public static void parse(Schema schema, Callback callback) {
        if (callback == null) {
            throw new NullPointerException("callback can't be null");
        }
        SchemaBuddy schemaBuddy = SchemaParser.parse(schema);
        if (schemaBuddy == null) {
            throw new IllegalStateException("Parsing schema failed for:" + schema.toString(true));
        }

        schemaBuddy.withCallback(callback);
    }

    public SchemaBuddy getParent() {
        return parent;
    }

    public SchemaBuddy getNotArrayParent() {
        if (parent == null) {
            return null;
        }
        if (parent.isArrayType()) {
            return parent.parent;
        }
        return parent;
    }

    private SchemaBuddy withCallback(Callback callback) {
        traverse(this, callback);
        return this;
    }

    public String getName() {
        return name;
    }

    public String getId() {
        // Use id from schema if we have it to avoid generating duplicates if same save repeated
        return (String) props.getOrDefault("id", uuidString);
    }

    public Object getProp(String prop) {
        return props.get(prop);
    }

    public Schema getSchema() {
        return schema;
    }

    public Schema.Type getType() {
        return schema.getType();
    }

    public boolean isOptional() {
        return optional;
    }

    public boolean isArrayType() {
        return getType() == Schema.Type.ARRAY;
    }

    boolean isRecordType() {
        return getType() == Schema.Type.RECORD;
    }

    public boolean isBranch() {
        if (parent == null) return true;
        if (isRecordType()) return true;
        return isArrayType();
    }

    public boolean isRoot() {
        if (parent != null) return false;
        if (!isRecordType()) {
            throw new IllegalStateException("root node should always be of type record:" + this.toString());
        }
        return true;
    }

    public boolean isSimpleType() {
        return !isArrayType() && getType() != Schema.Type.RECORD;
    }

    public List<SchemaBuddy> getChildren() {
        return children;
    }

    public List<SchemaBuddy> getComplexTypeChildren() {
        return children.stream().filter(SchemaBuddy::isBranch).collect(Collectors.toList());
    }

    public List<SchemaBuddy> getSimpleTypeChildren() {
        return children.stream().filter(SchemaBuddy::isSimpleType).collect(Collectors.toList());
    }

    SchemaBuddy getArrayTypeSchema() {
        if (parent == null) {
            throw new NullPointerException("parent can't be null");
        }
        if (getType() != Schema.Type.ARRAY) {
            throw new IllegalStateException("Parent need to be of type ARRAY, was " + getType());
        }
        if (children.size() != 1) {
            throw new IllegalStateException("Can only be one element as child when we have Array type as parent:" + this.toString());
        }
        return children.get(0);
    }


    public SchemaBuddy getChild(String childName) {
        Optional<SchemaBuddy> child = children.stream().filter(schemaWrapper -> schemaWrapper.name.equals(childName)).findFirst();
        return child.orElseThrow(() -> new RuntimeException("Could not find:" + childName + " in " + children.toString()));
    }

    private void addChild(SchemaBuddy schemaBuddy) {
        schemaBuddy.parent = this;
        children.add(schemaBuddy);
    }

    public String toString(boolean recursive) {
        StringBuilder sb = new StringBuilder();

        if (recursive) {
            sb.append(String.format("%s%s: %s optional:%s nullable:%s%n", getIntendString(), name, getType().getName(), optional, nullable));
            for (SchemaBuddy child : children) {
                sb.append(child.toString(true));
            }
        } else {
            sb.append(String.format("%s: %s optional:%s nullable:%s", name, getType().getName(), optional, nullable));
        }

        return sb.toString();
    }

    private void traverse(SchemaBuddy schemaBuddy, Callback callback) {
        if (schemaBuddy.isArrayType()) {
            assert schemaBuddy.children.size() == 1;
            traverse(schemaBuddy.children.get(0), callback);
            return;
        }

        callback.onTraverse(schemaBuddy);
        for (SchemaBuddy child : schemaBuddy.children) {
            traverse(child, callback);
        }
    }

    @Override
    public String toString() {
        return toString(false);
    }

    String toZeppelinPrintSchemaString() {
        StringBuilder sb = new StringBuilder();
        // Make it print out like schema in zeppelin for easy compare
        String typeName = getType().getName().equals("record") ? "struct" : getType().getName();
        sb.append(String.format("%s%s: %s (nullable = true)%n", getIntendString(), name, typeName));
        for (SchemaBuddy child : children) {
            sb.append(child.toZeppelinPrintSchemaString());
        }
        return sb.toString();
    }

    public String toLevelString() {
        return getIntendString() + toString();
    }

    String getIntendString() {
        int size = getParents().size();
        if (size == 0) return "";
        if (size == 1) return " |-- ";
        return String.join("", Collections.nCopies(size - 1, " |   ")) + " |-- ";
    }

    private List<String> getParents() {
        SchemaBuddy currentParent = parent;
        List<String> parentList = new ArrayList<>();
        while (currentParent != null) {
            parentList.add(currentParent.name);
            currentParent = currentParent.parent;
        }
        return parentList;
    }

    private static class SchemaParser {

        private SchemaParser() {
        }

        private static SchemaBuddy parse(Schema schema) {
            SchemaParser schemaParser = new SchemaParser();
            return schemaParser.mapRecursiveSchemas(schema.getName(), schema, schema.getObjectProps(), null, 0, false, false);
        }

        private SchemaBuddy mapRecursiveSchemas(String name, Schema schema, Map<String, Object> props, SchemaBuddy schemaBuddy, int level, boolean optional, boolean nullable) {
            switch (schema.getType()) {
                case UNION:
                    processUnion(name, schema, props, schemaBuddy, level);
                    break;
                case ARRAY:
                    if (schemaBuddy == null) {
                        throw new IllegalStateException("Parent SchemaBuddy can't be null when schemaType==ARRAY");
                    }
                    SchemaBuddy schemaBuddyChild = new SchemaBuddy(schema, name, props, optional);
                    schemaBuddy.addChild(schemaBuddyChild);
                    mapRecursiveSchemas(name, schema.getElementType(), schema.getObjectProps(), schemaBuddyChild, level + 1, optional, nullable);
                    return schemaBuddyChild;
                case RECORD:
                    SchemaBuddy schemaBuddyRecordChild = new SchemaBuddy(schema, name, props, optional);
                    if (schemaBuddy != null) {
                        schemaBuddy.addChild(schemaBuddyRecordChild);
                    }
                    for (Schema.Field field : schema.getFields()) {
                        mapRecursiveSchemas(field.name(), field.schema(), field.getObjectProps(), schemaBuddyRecordChild, level + 1, isOptional(field), nullable);
                    }
                    return schemaBuddyRecordChild;
                default:
                    processSimpleTypes(name, schema, props, schemaBuddy, optional, nullable);
            }
            return schemaBuddy;
        }

        private boolean isOptional(Schema.Field field) {
            if (field.schema().getType() == Schema.Type.UNION) {
                return field.schema().getTypes().get(0).getType() == Schema.Type.NULL;
            }
            return false;
        }

        private void processUnion(String name, Schema schema, Map<String, Object> props, SchemaBuddy schemaBuddy, int level) {
            List<Schema> types = schema.getTypes();
            Schema schema1 = types.get(0);
            Schema schema2 = types.get(1);
            boolean isOptional = schema1.getType() == Schema.Type.NULL;
            boolean isNullable = schema2.getType() == Schema.Type.NULL;
            if (isOptional) {
                mapRecursiveSchemas(name, schema2, props, schemaBuddy, level, true, false);
            } else if (isNullable) {
                mapRecursiveSchemas(name, schema1, props, schemaBuddy, level, false, true);
            } else {
                throw new IllegalStateException("Could not decode UNION:" + schema.getTypes());
            }
        }

        private void processSimpleTypes(String name, Schema schema, Map<String, Object> props, SchemaBuddy schemaBuddy, boolean optional, boolean nullable) {
            SchemaBuddy schemaBuddyChild = new SchemaBuddy(schema, name, props, optional, nullable);
            schemaBuddy.addChild(schemaBuddyChild);
        }
    }

    public interface Callback {
        void onTraverse(SchemaBuddy schemaBuddy);
    }
}
