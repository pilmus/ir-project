package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.Index;

import java.util.List;

public class IdfFeature extends AbstractFeature {
    @Override
    public double score(List<String> queryTerms, Document document, Collection collection) {
        long C = collection.getSize();

        double[] idfs = new double[queryTerms.size()];

        for (int i = 0; i < queryTerms.size(); i++) {
            double df = collection.getDocumentFrequency(queryTerms.get(i));

            idfs[i] = Math.log((C - df + 0.5) / (df + 0.5));
        }

        return sum(idfs);
    }
}
