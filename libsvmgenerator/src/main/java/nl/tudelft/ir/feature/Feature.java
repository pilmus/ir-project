package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;

import java.util.List;

public interface Feature {
    double score(List<String> queryTerms, Document document, Collection collection);
}
