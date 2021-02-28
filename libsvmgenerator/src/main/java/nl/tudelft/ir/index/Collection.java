package nl.tudelft.ir.index;

import java.util.Map;

public class Collection {
    private final Index index;

    private final long size;
    private final long totalTermCount;

    private final Map<String, Long> collectionFrequenciesCache;
    private final Map<String, Integer> documentFrequenciesCache;

    public Collection(Index index, Map<String, Long> collectionFrequenciesCache, Map<String, Integer> documentFrequenciesCache) {
        this.index = index;
        this.size = index.getCollectionSize();
        this.totalTermCount = index.getTotalTermCount();
        this.collectionFrequenciesCache = collectionFrequenciesCache;
        this.documentFrequenciesCache = documentFrequenciesCache;
    }

    public long getSize() {
        return size;
    }

    public long getTotalTermCount() {
        return totalTermCount;
    }

    public long getFrequency(String term) {
        Long frequency = collectionFrequenciesCache.get(term);

        if (frequency != null) {
            return frequency;
        }

        return index.getCollectionFrequency(term);
    }

    public int getDocumentFrequency(String term) {
        Integer frequency = documentFrequenciesCache.get(term);

        if (frequency != null) {
            return frequency;
        }

        return index.getDocumentFrequency(term);
    }
}
