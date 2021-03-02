package nl.tudelft.ir.index;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Collection {
    private final Index index;
    private final long numDocuments;
    private final long totalTermCount;
    private final double averageDocumentLength;

    private final ConcurrentMap<String, Long> collectionFrequencies = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> documentFrequencies = new ConcurrentHashMap<>();

    public Collection(Index index) {
        this.numDocuments = index.getNumDocuments();
        this.totalTermCount = index.getTotalTermCount();
        this.averageDocumentLength = index.getAverageDocumentLength();
        this.index = index;
    }

    public long getNumDocuments() {
        return numDocuments;
    }

    public long getTotalTermCount() {
        return totalTermCount;
    }

    public long getCollectionFrequency(String term) {
        return collectionFrequencies.computeIfAbsent(term, index::getCollectionFrequency);
    }

    public int getDocumentFrequency(String term) {
        return documentFrequencies.computeIfAbsent(term, index::getDocumentFrequency);
    }

    public double getAverageDocumentLength() {
        return averageDocumentLength;
    }
}
