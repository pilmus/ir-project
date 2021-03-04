package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.Index;

import java.util.List;

public class Bm25Feature extends AbstractFeature {
    private final float k1;
    private final float b;

    public Bm25Feature(float k1, float b) {
        this.k1 = k1;
        this.b = b;
    }

    @Override
    public double score(List<String> queryTerms, Document document, Collection collection) {
        double averageDocumentLength = collection.getAverageDocumentLength();

        double[] termScores = new double[queryTerms.size()];

        for (int i = 0; i < queryTerms.size(); i++) {
            String term = queryTerms.get(i);
            double idf = idf(term, collection);
            long termFrequency = document.getFrequency(term);
            termScores[i] = idf * (termFrequency / (termFrequency + (k1 * (1 - b + b * (document.getLength() / averageDocumentLength)))));
        }

        return sum(termScores);
    }

    private double idf(String term, Collection collection) {
        double df = collection.getDocumentFrequency(term);

        return Math.log(((collection.getNumDocuments() - df + 0.5) / (df + 0.5)) + 1);
    }
}
