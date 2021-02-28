package nl.tudelft.ir.index;

import java.util.Map;

public class Collection {

    private final long numDocuments;
    private final long totalTermCount;

    private final Map<String, Long> collectionFrequenciesCache;
    private final Map<String, Integer> documentFrequenciesCache;

    public Collection(long numDocuments, long totalTermCount, Map<String, Long> collectionFrequenciesCache, Map<String, Integer> documentFrequenciesCache) {
        this.numDocuments = numDocuments;
        this.totalTermCount = totalTermCount;
        this.collectionFrequenciesCache = collectionFrequenciesCache;
        this.documentFrequenciesCache = documentFrequenciesCache;
    }

    public long getNumDocuments() {
        return numDocuments;
    }

    public long getTotalTermCount() {
        return totalTermCount;
    }

    public long getCollectionFrequency(String term) {
        Long frequency = collectionFrequenciesCache.get(term);

        if (frequency == null) {
            throw new RuntimeException("Could not find collection frequency of term " + term);
        }

        return frequency;
    }

    public int getDocumentFrequency(String term) {
        Integer frequency = documentFrequenciesCache.get(term);

        if (frequency == null) {
            throw new RuntimeException("Could not find document frequency of term " + term);
        }

        return frequency;
    }
}
