package nl.tudelft.ir;

import ciir.umass.edu.learning.DataPoint;
import ciir.umass.edu.learning.tree.Ensemble;
import ciir.umass.edu.parsing.ModelLineProducer;
import io.anserini.index.IndexReaderUtils;
import io.anserini.rerank.Reranker;
import io.anserini.rerank.RerankerContext;
import io.anserini.rerank.ScoredDocuments;
import nl.tudelft.ir.feature.Feature;
import nl.tudelft.ir.feature.Features;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;
import nl.tudelft.ir.index.Index;
import nl.tudelft.ir.index.LuceneIndex;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class LambdaMARTReranker implements Reranker<Integer> {

    private final List<Feature> features;
    private final Ensemble ensemble;

    public LambdaMARTReranker(List<Feature> features) {
        this.features = features;

        // Read model
        String fullText;
        try {
            fullText = Files.readString(Paths.get("/media/hd/ir-project/models/lambdamart"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ModelLineProducer lineByLine = new ModelLineProducer();
        lineByLine.parse(fullText, (model, endEns) -> {
        });

        ensemble = new Ensemble(lineByLine.getModel().toString());
    }

    @Override
    public ScoredDocuments rerank(ScoredDocuments docs, RerankerContext<Integer> context) {
        IndexReader reader = context.getIndexSearcher().getIndexReader();
        Index index = new LuceneIndex(reader);
        DocumentCollection documentCollection = new DocumentCollection(index);

        List<String> queryTerms = context.getQueryTokens();

        Map<Integer, Float> documentIndexToScore = new HashMap<>();

        for (int i = 0; i < docs.documents.length; i++) {
            int luceneDocId = docs.ids[i];
            String docId = IndexReaderUtils.convertLuceneDocidToDocid(reader, luceneDocId);
            Document document = index.retrieveById(docId);

            DataPoint dataPoint = createDataPoint(queryTerms, document, documentCollection);

            float score = ensemble.eval(dataPoint);
            documentIndexToScore.put(i, score);
        }

        List<Integer> documentIndicesToKeep = documentIndexToScore.entrySet().stream()
                .sorted(Collections.reverseOrder(Comparator.comparingDouble(Map.Entry::getValue)))
                .map(Map.Entry::getKey)
                .limit(100)
                .collect(Collectors.toList());

        ScoredDocuments rescored = new ScoredDocuments();
        rescored.scores = new float[documentIndicesToKeep.size()];
        rescored.ids = new int[documentIndicesToKeep.size()];
        rescored.documents = new org.apache.lucene.document.Document[documentIndicesToKeep.size()];

        int i = 0;
        for (int indexToKeep : documentIndicesToKeep) {
            rescored.scores[i] = documentIndexToScore.get(indexToKeep);
            rescored.ids[i] = docs.ids[indexToKeep];
            rescored.documents[i] = docs.documents[indexToKeep];
            i++;
        }

        return rescored;
    }

    private DataPoint createDataPoint(List<String> queryTerms, Document document, DocumentCollection documentCollection) {
        float[] featureVec = Features.generateVector(features, queryTerms, document, documentCollection);

        DataPoint dp = new FeatureVectorDataPoint(featureVec);
        dp.setFeatureVector(featureVec);

        return dp;
    }

    @Override
    public String tag() {
        return "lambdaMART";
    }

    private static class FeatureVectorDataPoint extends DataPoint {

        private float[] featureVector;

        public FeatureVectorDataPoint(float[] featureVector) {
            this.featureVector = featureVector;
        }

        @Override
        public float getFeatureValue(int fid) {
            return this.featureVector[fid - 1]; // 1 indexed for some reason
        }

        @Override
        public void setFeatureValue(int fid, float fval) {
            this.featureVector[fid - 1] = fval;
        }

        @Override
        public float[] getFeatureVector() {
            return featureVector;
        }

        @Override
        public void setFeatureVector(float[] dfVals) {
            this.featureVector = dfVals;
        }
    }
}
