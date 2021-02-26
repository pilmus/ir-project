package nl.tudelft.ir.feature;

import org.apache.lucene.index.IndexReader;

import java.util.List;
import java.util.Map;

public class TfFeature extends AbstractFeature {

    public TfFeature(IndexReader reader) {
        super(reader);
    }

    @Override
    public double score(List<String> queryTerms, String docId) {
        double[] tfs = new double[queryTerms.size()];
        Map<String, Long> docVector = getDocumentVector(docId);

        for (int i = 0; i < queryTerms.size(); i++) {
            tfs[i] = docVector.getOrDefault(queryTerms.get(i), 0L);
        }

        return sum(tfs);
    }
}
