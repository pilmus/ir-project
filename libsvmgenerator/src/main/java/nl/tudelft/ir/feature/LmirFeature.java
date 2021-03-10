package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.lang.Math.max;

public class LmirFeature extends AbstractFeature {
    private final Function<Document, Document.Field> mapper;
    private final SmoothingMethod smoothingMethod;

    public LmirFeature(Function<Document, Document.Field> mapper, SmoothingMethod smoothingMethod) {
        this.mapper = mapper;
        this.smoothingMethod = smoothingMethod;
    }

    @Override
    public float score(List<String> queryTerms, Document document, DocumentCollection documentCollection) {
        Document.Field field = mapper.apply(document);

        Map<String, Long> documentVector = field.getVector();
        long documentLength = field.getLength();

        List<String> documentTerms = field.getTerms();
        Map<String, Double> lmirProbabilities = new HashMap<>(documentTerms.size());

        long totalTermCount = documentCollection.getTotalTermCount();

        double summedCorpusProbabilities = 0;
        double summedLmirProbabilities = 0;

        for (String term : documentTerms) {
            double corpusProbabilityForTerm = documentCollection.getCollectionFrequency(term) / (double) totalTermCount;
            double lmirProbabilityForTerm = smoothingMethod.smooth(term, documentVector, corpusProbabilityForTerm, documentLength);

            lmirProbabilities.put(term, lmirProbabilityForTerm);

            summedCorpusProbabilities += corpusProbabilityForTerm;
            summedLmirProbabilities += lmirProbabilityForTerm;
        }

        double alpha = (1 - summedLmirProbabilities) / (1 - summedCorpusProbabilities);

        double score = 1;
        for (String term : queryTerms) {
            if (lmirProbabilities.containsKey(term)) {
                score *= lmirProbabilities.get(term);
            } else {
                score *= alpha * documentCollection.getCollectionFrequency(term) / totalTermCount;
            }
        }

        return (float) score;
    }

    interface SmoothingMethod {
        double smooth(String term, Map<String, Long> documentVector, double corpusProbability, long documentLength);
    }

    public static class JelinekMercerSmoothing implements SmoothingMethod {

        private final double beta;

        public JelinekMercerSmoothing(double beta) {
            this.beta = beta;
        }

        @Override
        public double smooth(String term, Map<String, Long> documentVector, double corpusProbability, long documentLength) {
            return (1 - beta) * documentVector.get(term) / documentLength + beta * corpusProbability;
        }
    }

    public static class DirichletPriorSmoothing implements SmoothingMethod {

        private final double mu;

        public DirichletPriorSmoothing(double mu) {
            this.mu = mu;
        }

        @Override
        public double smooth(String term, Map<String, Long> documentVector, double corpusProbability, long documentLength) {
            return (documentVector.get(term) + mu * corpusProbability) / (documentLength + mu);
        }
    }

    public static class AbsoluteDiscountingSmoothing implements SmoothingMethod {

        private final double discountingFactor;

        public AbsoluteDiscountingSmoothing(double discountingFactor) {
            this.discountingFactor = discountingFactor;
        }

        @Override
        public double smooth(String term, Map<String, Long> documentVector, double corpusProbability, long documentLength) {
            double sigma = discountingFactor * (documentVector.size() / (double) documentLength);
            return max(documentVector.get(term) - discountingFactor, 0) / documentLength + sigma * corpusProbability;
        }
    }

}
