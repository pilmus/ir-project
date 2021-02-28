package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.Index;

import java.util.List;

public class Bm25Feature extends AbstractFeature {
    private static final float k1 = 0.9f;
    private static final float b = 0.4f;

    private final Index index;

    public Bm25Feature(Index index) {
        this.index = index;
    }

    @Override
    public double score(List<String> queryTerms, Document document, Collection collection) {
        double[] termScores = new double[queryTerms.size()];

        for (int i = 0; i < queryTerms.size(); i++) {
            termScores[i] = index.getBm25Weight(queryTerms.get(i), document.getId(), k1, b);
        }

        return sum(termScores);
    }
}
