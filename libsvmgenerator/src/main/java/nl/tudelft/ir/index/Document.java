package nl.tudelft.ir.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Document {
    private final String id;
    private final Map<String, Long> vector;
    private final long length;
    private final List<String> terms;

    public Document(String id, Map<String, Long> vector, long length) {
        this.id = id;
        this.vector = vector;
        this.length = length;
        terms = new ArrayList<>(vector.keySet());
    }

    public String getId() {
        return id;
    }

    public Map<String, Long> getVector() {
        return vector;
    }

    public long getFrequency(String term) {
        return vector.getOrDefault(term, 0L);
    }

    public List<String> getTerms() {
        return terms;
    }

    public long getLength() {
        return length;
    }
}
