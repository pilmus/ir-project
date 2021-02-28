package nl.tudelft.ir.index;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class Document {
    private final String id;
    private final Map<String, Long> vector;
    private final long length;

    public Document(String id, Map<String, Long> vector, long length) {
        this.id = id;
        this.vector = vector;
        this.length = length;
    }

    public String getId() {
        return id;
    }

    public Map<String, Long> getVector() {
        return vector;
    }

    public Set<String> getTerms() {
        return vector.keySet();
    }

    public long getLength() {
        return length;
    }
}
