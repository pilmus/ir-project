package nl.tudelft.ir.feature;

import io.anserini.index.IndexReaderUtils;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.List;

public class Bm25Feature extends AbstractFeature {
    private static final float k1 = 0.9f;
    private static final float b = 0.4f;

    public Bm25Feature(IndexReader reader) {
        super(reader);
    }

    @Override
    public double score(List<String> queryTerms, String docId) {
        double[] termScores = new double[queryTerms.size()];

        for (int i = 0; i < queryTerms.size(); i++) {
            termScores[i] = getBm25(queryTerms.get(i), docId);
        }

        return sum(termScores);
    }

    private double getBm25(String term, String docId) {
        try {
            return IndexReaderUtils.getBM25AnalyzedTermWeightWithParameters(getReader(), docId, term, k1, b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
