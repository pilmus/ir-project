package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;

import java.util.List;

public class TfIdfFeature extends AbstractFeature {
    @Override
    public float score(List<String> queryTerms, Document document, Collection collection) {
        double[] idfs = new double[queryTerms.size()];
        double[] tfs = new double[queryTerms.size()];

        for (int i = 0; i < queryTerms.size(); i++) {
            String term = queryTerms.get(i);

            tfs[i] = document.getFrequency(term);
            idfs[i] = IdfFeature.idf(term, collection);
        }

        double[] tfidfs = multiply(tfs, idfs);

        return (float) sum(tfidfs);
    }
}
