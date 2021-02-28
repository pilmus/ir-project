package nl.tudelft.ir.index;

import java.util.Map;

public interface Index {
    Map<String, Long> getDocumentVector(String docId);
    long getDocumentLength(String docId);
    double getBm25Weight(String term, String docId, float k1, float b);

    long getCollectionFrequency(String term);
    int getCollectionSize();
    long getTotalTermCount();
}
