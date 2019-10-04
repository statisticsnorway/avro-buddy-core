package no.ssb.avro.convert.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.HashMap;
import java.util.Collections;

/**
 * DataElement is used when mapping data from any input source
 * When we have data represented as DataElements we can use {@link no.ssb.avro.convert.core.SchemaAwareElement}
 * to produce {@link org.apache.avro.generic.GenericRecord}
 */
public class DataElement {

    final String name;
    private DataElement parent;
    private String value;
    private final List<DataElement> children = new ArrayList<>();

    public DataElement(String name) {
        this.name = name;
    }

    public DataElement(String name, String value) {
        this(name);
        this.value = value;
    }

    public String getName() {
        return name;
    }

    String getValue() {
        return value;
    }

    List<DataElement> getChildren() {
        return children;
    }

    public DataElement findChildByName(String name) {
        Optional<DataElement> element = children.stream().filter(subElement -> subElement.getName().equals(name)).findFirst();
        return element.orElse(null);
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void addChild(DataElement child) {
        child.parent = this;
        children.add(child);
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean recursive) {
        StringBuilder sb = new StringBuilder();
        if (recursive) {
            sb.append(String.format("%s%s value:%s\n", getIntendString(), name, value));

            Map<String, List<DataElement>> map = new HashMap<>();
            for (DataElement child : children) {
                sb.append(child.toString(true));
            }
        } else {
            sb.append(String.format("%s value:%s", name, value));
        }
        return sb.toString();
    }

    private String getIntendString() {
        int size = getParents().size();
        if (size == 0) return "";
        if (size == 1) return " |-- ";

        return String.join("", Collections.nCopies(size - 1, " |   ")) + " |-- ";
    }

    private List<String> getParents() {
        DataElement currentParent = parent;
        List<String> parentList = new ArrayList<>();
        while (currentParent != null) {
            parentList.add(currentParent.name);
            currentParent = currentParent.parent;
        }
        return parentList;
    }
}
