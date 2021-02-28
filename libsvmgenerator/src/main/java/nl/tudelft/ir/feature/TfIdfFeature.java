package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.Index;

import java.util.List;
import java.util.Map;

public class TfIdfFeature extends AbstractFeature {
    @Override
    public double score(List<String> queryTerms, Document document, Collection collection) {
        long C = collection.getSize();

        double[] idfs = new double[queryTerms.size()];
        double[] tfs = new double[queryTerms.size()];

        Map<String, Long> docVector = document.getVector();

        for (int i = 0; i < queryTerms.size(); i++) {
            tfs[i] = docVector.getOrDefault(queryTerms.get(i), 0L);

            double df = collection.getFrequency(queryTerms.get(i));
            idfs[i] = Math.log((C - df + 0.5) / (df + 0.5));
        }

        double[] tfidfs = multiply(tfs, idfs);

        return sum(tfidfs);
    }
}
