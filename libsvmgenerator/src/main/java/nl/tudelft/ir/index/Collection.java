package nl.tudelft.ir.index;

import java.util.Map;

public class Collection {
    private final Index index;

    private final long size;
    private final long totalTermCount;

    private final Map<String, Long> frequenciesCache;

    public Collection(Index index, Map<String, Long> frequenciesCache) {
        this.index = index;
        this.size = index.getCollectionSize();
        this.totalTermCount = index.getTotalTermCount();
        this.frequenciesCache = frequenciesCache;
    }

    public long getSize() {
        return size;
    }

    public long getTotalTermCount() {
        return totalTermCount;
    }

    public long getFrequency(String term) {
        Long frequency = frequenciesCache.get(term);

        if (frequency != null) {
            return frequency;
        }

        return index.getCollectionFrequency(term);
    }
}
