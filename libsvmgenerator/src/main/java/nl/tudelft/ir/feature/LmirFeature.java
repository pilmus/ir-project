package nl.tudelft.ir.feature;

import org.apache.lucene.index.IndexReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Math.max;

public class LmirFeature extends AbstractFeature {
    private final SmoothingMethod smoothingMethod;

    public LmirFeature(IndexReader reader, SmoothingMethod smoothingMethod) {
        super(reader);
        this.smoothingMethod = smoothingMethod;
    }

    @Override
    public double score(List<String> queryTerms, String docId) {
        Map<String, Long> documentVector = getDocumentVector(docId);
        long documentLength = getDocumentLength(docId);

        List<String> documentTerms = documentVector.keySet().stream().filter(k -> documentVector.get(k) != null).collect(Collectors.toList());
        Map<String, Double> corpusProbabilities = documentTerms.stream().collect(Collectors.toMap(Function.identity(), s -> 0d));
        Map<String, Double> lmirProbabilities = new HashMap<>(corpusProbabilities);

        long totalTermCount = getTotalTermCount();

        for (String term : lmirProbabilities.keySet()) {
            double corpusProbabilityForTerm = getCollectionFrequency(term) / (double) totalTermCount;
            double lmirProbabilityForTerm = smoothingMethod.smooth(term, documentVector, corpusProbabilityForTerm, documentLength);

            corpusProbabilities.put(term, corpusProbabilityForTerm);
            lmirProbabilities.put(term, lmirProbabilityForTerm);
        }

        double summedCorpusProbabilities = corpusProbabilities.values().stream().mapToDouble(d -> d).sum();
        double summedLmirProbabilities = lmirProbabilities.values().stream().mapToDouble(d -> d).sum();
        double alpha = (1 - summedLmirProbabilities) / (1 - summedCorpusProbabilities);

        double score = 1;
        for (String term : queryTerms) {
            if (lmirProbabilities.containsKey(term)) {
                score *= lmirProbabilities.get(term);
            } else {
                score *= alpha * getCollectionFrequency(term) / totalTermCount;
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
