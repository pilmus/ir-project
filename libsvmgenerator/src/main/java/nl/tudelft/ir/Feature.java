package nl.tudelft.ir;

import java.util.List;

public interface Feature {
    float score(List<String> queryTerms, String docId);
}
