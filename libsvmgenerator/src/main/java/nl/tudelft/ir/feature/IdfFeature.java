package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;

import java.util.List;

public class IdfFeature extends AbstractFeature {
    public static double idf(String term, Collection collection) {
        double df = collection.getDocumentFrequency(term);

        return Math.log(((collection.getNumDocuments() - df + 0.5) / (df + 0.5)) + 1);
    }

    @Override
    public double score(List<String> queryTerms, Document document, Collection collection) {
        double[] idfs = new double[queryTerms.size()];

        for (int i = 0; i < queryTerms.size(); i++) {
            idfs[i] = idf(queryTerms.get(i), collection);
        }

        return sum(idfs);
    }
}
