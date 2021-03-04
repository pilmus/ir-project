package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;

import java.util.List;

public class DocumentLengthFeature implements Feature {
    @Override
    public float score(List<String> queryTerms, Document document, Collection collection) {
        return document.getLength();
    }
}
