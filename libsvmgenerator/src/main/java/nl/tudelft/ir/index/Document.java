package nl.tudelft.ir.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Document {
    private final String id;

    private final Field wholeDocument;
    private final Field title;
    private final Field url;
    private final Field body;

    public Document(String id, Field wholeDocument, Field title, Field url, Field body) {
        this.id = id;
        this.wholeDocument = wholeDocument;
        this.title = title;
        this.url = url;
        this.body = body;
    }

    public String getId() {
        return id;
    }

    public Field getTitle() {
        return title;
    }

    public Field getUrl() {
        return url;
    }

    public Field getBody() {
        return body;
    }

    public Field getWholeDocument() {
        return wholeDocument;
    }

    public static class Field {
        private final String contents;
        private final Map<String, Long> vector;
        private final List<String> terms;

        public Field(String contents, Map<String, Long> vector) {
            this.contents = contents;
            this.vector = vector;
            terms = new ArrayList<>(vector.keySet());
        }

        public String getContents() {
            return contents;
        }

        public long getFrequency(String term) {
            return vector.getOrDefault(term, 0L);
        }

        public long getLength() {
            return contents.length();
        }

        public Map<String, Long> getVector() {
            return vector;
        }

        public List<String> getTerms() {
            return terms;
        }
    }
}
