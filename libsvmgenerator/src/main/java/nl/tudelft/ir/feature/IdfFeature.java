package nl.tudelft.ir.feature;

import org.apache.lucene.index.IndexReader;

import java.util.List;

public class IdfFeature extends AbstractFeature {
    public IdfFeature(IndexReader reader) {
        super(reader);
    }

    @Override
    public double score(List<String> queryTerms, String docId) {
        long C = this.numDocuments();

        double[] idfs = new double[queryTerms.size()];

        for (int i = 0; i < queryTerms.size(); i++) {
            double df = getCollectionFrequency(queryTerms.get(i));

            idfs[i] = Math.log((C - df + 0.5) / (df + 0.5));
        }

        return sum(idfs);
    }
}
