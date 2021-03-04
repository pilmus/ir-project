package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;

import java.util.Arrays;
import java.util.List;

abstract public class Features {

    public final static List<Feature> LAMBDAMART_DEFAULT_FEATURES = Arrays.asList(
            /* feature index */
            /* 0             */ new DocumentLengthFeature(),
            /* 1             */ new TfFeature(),
            /* 2             */ new IdfFeature(),
            /* 3             */ new TfIdfFeature(),
            /* 4             */ new Bm25Feature(1.2f, 0.75f),
            /* 5             */ new LmirFeature(new LmirFeature.JelinekMercerSmoothing(0.1)),
            /* 6             */ new LmirFeature(new LmirFeature.DirichletPriorSmoothing(2000)),
            /* 7             */ new LmirFeature(new LmirFeature.AbsoluteDiscountingSmoothing(0.7))
    );

    private Features() {
    }

    public static float[] generateVector(List<Feature> features, List<String> queryTerms, Document document, Collection collection) {
        float[] featureVec = new float[features.size()];

        for (int i = 0; i < features.size(); i++) {
            featureVec[i] = features.get(i).score(queryTerms, document, collection);
        }

        return featureVec;
    }
}
