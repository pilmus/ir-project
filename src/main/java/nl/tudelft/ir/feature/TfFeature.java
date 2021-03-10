package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;

import java.util.List;
import java.util.function.Function;

public class TfFeature extends AbstractFeature {

    private final Function<Document, Document.Field> mapper;

    public TfFeature(Function<Document, Document.Field> mapper) {
        this.mapper = mapper;
    }

    @Override
    public float score(List<String> queryTerms, Document document, DocumentCollection documentCollection) {
        double[] tfs = new double[queryTerms.size()];

        Document.Field field = mapper.apply(document);

        for (int i = 0; i < queryTerms.size(); i++) {
            tfs[i] = field.getFrequency(queryTerms.get(i));
        }

        return (float) sum(tfs);
    }
}
