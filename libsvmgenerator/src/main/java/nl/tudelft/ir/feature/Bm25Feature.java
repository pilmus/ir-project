package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;

import java.util.List;

public class Bm25Feature extends AbstractFeature {
    private final float k1;
    private final float b;

    public Bm25Feature(float k1, float b) {
        this.k1 = k1;
        this.b = b;
    }

    @Override
    public float score(List<String> queryTerms, Document document, DocumentCollection documentCollection) {
        double averageDocumentLength = documentCollection.getAverageDocumentLength();

        double[] termScores = new double[queryTerms.size()];

        Document.Field wholeDocument = document.getWholeDocument();

        for (int i = 0; i < queryTerms.size(); i++) {
            String term = queryTerms.get(i);
            double idf = IdfFeature.idf(term, documentCollection);
            long termFrequency = wholeDocument.getFrequency(term);
            termScores[i] = idf * (termFrequency / (termFrequency + (k1 * (1 - b + b * (wholeDocument.getLength() / averageDocumentLength)))));
        }

        return (float) sum(termScores);
    }
}
