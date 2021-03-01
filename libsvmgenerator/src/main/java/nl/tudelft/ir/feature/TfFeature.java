package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;

import java.util.List;

public class TfFeature extends AbstractFeature {
    @Override
    public double score(List<String> queryTerms, Document document, Collection collection) {
        double[] tfs = new double[queryTerms.size()];

        for (int i = 0; i < queryTerms.size(); i++) {
            tfs[i] = document.getFrequency(queryTerms.get(i));
        }

        return sum(tfs);
    }
}
