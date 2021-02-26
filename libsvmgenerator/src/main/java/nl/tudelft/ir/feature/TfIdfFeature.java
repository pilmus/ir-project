package nl.tudelft.ir.feature;

import org.apache.lucene.index.IndexReader;

import java.util.List;
import java.util.Map;

public class TfIdfFeature extends AbstractFeature {
    public TfIdfFeature(IndexReader reader) {
        super(reader);
    }

    @Override
    public double score(List<String> queryTerms, String docId) {
        long C = this.numDocuments();

        double[] idfs = new double[queryTerms.size()];
        double[] tfs = new double[queryTerms.size()];

        Map<String, Long> docVector = getDocumentVector(docId);

        for (int i = 0; i < queryTerms.size(); i++) {
            tfs[i] = docVector.getOrDefault(queryTerms.get(i), 0L);

            double df = getCollectionFrequency(queryTerms.get(i));
            idfs[i] = Math.log((C - df + 0.5) / (df + 0.5));
        }

        double[] tfidfs = multiply(tfs, idfs);

        return sum(tfidfs);
    }
}
