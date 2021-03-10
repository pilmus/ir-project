package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;

import java.util.List;
import java.util.function.Function;

public class TfIdfFeature extends AbstractFeature {

    private final Function<Document, Document.Field> mapper;

    public TfIdfFeature(Function<Document, Document.Field> mapper) {
        this.mapper = mapper;
    }

    @Override
    public float score(List<String> queryTerms, Document document, DocumentCollection documentCollection) {
        double[] idfs = new double[queryTerms.size()];
        double[] tfs = new double[queryTerms.size()];

        Document.Field field = mapper.apply(document);

        for (int i = 0; i < queryTerms.size(); i++) {
            String term = queryTerms.get(i);

            tfs[i] = field.getFrequency(term);
            idfs[i] = IdfFeature.idf(term, documentCollection);
        }

        double[] tfidfs = multiply(tfs, idfs);

        return (float) sum(tfidfs);
    }
}
