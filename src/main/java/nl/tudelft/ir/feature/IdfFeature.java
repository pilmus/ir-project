package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;

import java.util.List;

public class IdfFeature extends AbstractFeature {
    public static double idf(String term, DocumentCollection documentCollection) {
        double df = documentCollection.getDocumentFrequency(term);

        return Math.log(((documentCollection.getNumDocuments() - df + 0.5) / (df + 0.5)) + 1);
    }

    @Override
    public float score(List<String> queryTerms, Document document, DocumentCollection documentCollection) {
        double[] idfs = new double[queryTerms.size()];

        for (int i = 0; i < queryTerms.size(); i++) {
            idfs[i] = idf(queryTerms.get(i), documentCollection);
        }

        return (float) sum(idfs);
    }
}
