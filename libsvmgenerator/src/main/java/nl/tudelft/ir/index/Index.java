package nl.tudelft.ir.index;

import java.util.Map;

public interface Index {
    Map<String, Long> getDocumentVector(String docId);

    long getCollectionFrequency(String term);

    int getDocumentFrequency(String term);

    int getNumDocuments();

    long getTotalTermCount();

    double getAverageDocumentLength();

    Document retrieveById(String docId);
}
