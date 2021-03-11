package nl.tudelft.ir;

import ciir.umass.edu.learning.DataPoint;
import ciir.umass.edu.learning.tree.LambdaMART;
import io.anserini.index.IndexReaderUtils;
import nl.tudelft.ir.feature.Feature;
import nl.tudelft.ir.feature.Features;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;
import nl.tudelft.ir.index.Index;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class LambdaMARTReranker {

    private final List<Feature> features;
    private final Index index;
    private final DocumentCollection documentCollection;
    private final IndexReader reader;
    private final LambdaMART lambdaMart;

    public LambdaMARTReranker(List<Feature> features, Index index, DocumentCollection documentCollection, IndexReader reader, String modelPath) {
        this.features = features;
        this.index = index;
        this.documentCollection = documentCollection;
        this.reader = reader;
        this.lambdaMart = readModel(modelPath);
    }

    private LambdaMART readModel(String modelPath) {
        LambdaMART lambdaMART = new LambdaMART();

        try {
            lambdaMART.loadFromString(Files.readString(Paths.get(modelPath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return lambdaMART;
    }

    public List<RankedDocument> rerankTopDocs(TopDocs topDocs, List<String> queryTerms) {
        Map<Document, Float> newScores = new HashMap<>();

        // Calculate LambdaMART scores for each document
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            int luceneDocId = scoreDoc.doc;
            String docId = IndexReaderUtils.convertLuceneDocidToDocid(reader, luceneDocId);
            Document document = index.retrieveById(docId);

            DataPoint dataPoint = createDataPoint(queryTerms, document, documentCollection);

            float score = (float) lambdaMart.eval(dataPoint);
            newScores.put(document, score);
        }

        // Sort by the newly assigned scores
        List<Document> orderedDocuments = newScores.entrySet().stream()
                .sorted(Collections.reverseOrder(Comparator.comparingDouble(Map.Entry::getValue)))
                .limit(100)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // Create a new ranking
        List<RankedDocument> rankedDocuments = new ArrayList<>(orderedDocuments.size());
        int rank = 1;
        for (Document orderedDocument : orderedDocuments) {
            rankedDocuments.add(new RankedDocument(rank, orderedDocument, newScores.get(orderedDocument)));

            rank++;
        }

        return rankedDocuments;
    }

    private DataPoint createDataPoint(List<String> queryTerms, Document document, DocumentCollection documentCollection) {
        float[] featureVec = Features.generateVector(features, queryTerms, document, documentCollection);

        return new FeatureVectorDataPoint(featureVec);
    }

    public static class RankedDocument {
        private final int rank;
        private final Document document;
        private final float score;

        public RankedDocument(int rank, Document document, float score) {
            this.rank = rank;
            this.document = document;
            this.score = score;
        }

        public int getRank() {
            return rank;
        }

        public Document getDocument() {
            return document;
        }

        public float getScore() {
            return score;
        }
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
