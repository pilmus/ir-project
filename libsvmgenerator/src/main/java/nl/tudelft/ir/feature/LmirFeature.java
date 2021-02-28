package nl.tudelft.ir.feature;

import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.max;

public class LmirFeature extends AbstractFeature {
    private final SmoothingMethod smoothingMethod;

    public LmirFeature(SmoothingMethod smoothingMethod) {
        this.smoothingMethod = smoothingMethod;
    }

    @Override
    public double score(List<String> queryTerms, Document document, Collection collection) {
        Map<String, Long> documentVector = document.getVector();
        long documentLength = document.getLength();

        Set<String> documentTerms = document.getTerms();
        Map<String, Double> lmirProbabilities = new HashMap<>();
        for (String term : documentTerms) {
            lmirProbabilities.put(term, 0d);
        }

        long totalTermCount = collection.getTotalTermCount();

        double summedCorpusProbabilities = 0;
        double summedLmirProbabilities = 0;

        for (String term : documentTerms) {
            double corpusProbabilityForTerm = collection.getCollectionFrequency(term) / (double) totalTermCount;
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
                score *= alpha * collection.getCollectionFrequency(term) / totalTermCount;
            }
        }

        return score;
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
