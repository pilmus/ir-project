package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.Index;

import java.util.List;

public class Bm25Feature extends AbstractFeature {
    private final float k1;
    private final float b;

    private final Index index;

    public Bm25Feature(Index index, float k1, float b) {
        this.index = index;
        this.k1 = k1;
        this.b = b;
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
