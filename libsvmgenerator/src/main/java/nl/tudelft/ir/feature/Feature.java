package nl.tudelft.ir.feature;

import java.util.List;

public interface Feature {
    double score(List<String> queryTerms, String docId);
}
