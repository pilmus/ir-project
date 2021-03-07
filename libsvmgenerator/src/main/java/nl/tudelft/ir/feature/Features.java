package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;

import java.util.Arrays;
import java.util.List;

abstract public class Features {

    public static final LmirFeature.JelinekMercerSmoothing JELINEK_MERCER_SMOOTHING = new LmirFeature.JelinekMercerSmoothing(0.1);

    public static final LmirFeature.DirichletPriorSmoothing DIRICHLET_PRIOR_SMOOTHING = new LmirFeature.DirichletPriorSmoothing(2000);

    public static final LmirFeature.AbsoluteDiscountingSmoothing ABSOLUTE_DISCOUNTING_SMOOTHING = new LmirFeature.AbsoluteDiscountingSmoothing(0.7);

    public final static List<Feature> LAMBDAMART_DEFAULT_FEATURES = Arrays.asList(
            new IdfFeature(),
            new Bm25Feature(1.2f, 0.75f),

            new TfFeature(Document::getBody),
            new TfFeature(Document::getTitle),
            new TfFeature(Document::getUrl),
            new TfFeature(Document::getWholeDocument),

            new TfIdfFeature(Document::getBody),
            new TfIdfFeature(Document::getTitle),
            new TfIdfFeature(Document::getUrl),
            new TfIdfFeature(Document::getWholeDocument),

            new LmirFeature(Document::getBody, JELINEK_MERCER_SMOOTHING),
            new LmirFeature(Document::getTitle, JELINEK_MERCER_SMOOTHING),
            new LmirFeature(Document::getUrl, JELINEK_MERCER_SMOOTHING),
            new LmirFeature(Document::getWholeDocument, JELINEK_MERCER_SMOOTHING),

            new LmirFeature(Document::getBody, DIRICHLET_PRIOR_SMOOTHING),
            new LmirFeature(Document::getTitle, DIRICHLET_PRIOR_SMOOTHING),
            new LmirFeature(Document::getUrl, DIRICHLET_PRIOR_SMOOTHING),
            new LmirFeature(Document::getWholeDocument, DIRICHLET_PRIOR_SMOOTHING),

            new LmirFeature(Document::getBody, ABSOLUTE_DISCOUNTING_SMOOTHING),
            new LmirFeature(Document::getTitle, ABSOLUTE_DISCOUNTING_SMOOTHING),
            new LmirFeature(Document::getUrl, ABSOLUTE_DISCOUNTING_SMOOTHING),
            new LmirFeature(Document::getWholeDocument, ABSOLUTE_DISCOUNTING_SMOOTHING),

            /* length of url */
            (queryTerms, document, documentCollection) -> document.getUrl().getLength(),

            /* length of title */
            (queryTerms, document, documentCollection) -> document.getTitle().getLength(),

            /* length of body */
            (queryTerms, document, documentCollection) -> document.getBody().getLength(),

            /* length of whole document */
            (queryTerms, document, documentCollection) -> document.getWholeDocument().getLength(),

            /* number of slashes in URL */
            (queryTerms, document, documentCollection) -> document.getUrl().getContents().codePoints().filter(ch -> ch == '/').count(),

            /* number of terms in query */
            (queryTerms, document, documentCollection) -> queryTerms.size()
    );

    private Features() {
    }

    public static float[] generateVector(List<Feature> features, List<String> queryTerms, Document document, DocumentCollection documentCollection) {
        float[] featureVec = new float[features.size()];

        for (int i = 0; i < features.size(); i++) {
            featureVec[i] = features.get(i).score(queryTerms, document, documentCollection);
        }

        return featureVec;
    }
}
