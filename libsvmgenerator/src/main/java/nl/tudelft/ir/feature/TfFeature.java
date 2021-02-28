package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;

import java.util.List;
import java.util.Map;

public class TfFeature extends AbstractFeature {
    @Override
    public double score(List<String> queryTerms, Document document, Collection collection) {
        double[] tfs = new double[queryTerms.size()];
        Map<String, Long> docVector = document.getVector();

        for (int i = 0; i < queryTerms.size(); i++) {
            tfs[i] = docVector.getOrDefault(queryTerms.get(i), 0L);
        }

        return sum(tfs);
    }
}
