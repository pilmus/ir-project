package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;

import java.util.List;

@FunctionalInterface
public interface Feature {
    float score(List<String> queryTerms, Document document, DocumentCollection documentCollection);
}
