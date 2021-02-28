package nl.tudelft.ir.feature;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.index.IndexArgs;
import io.anserini.index.IndexReaderUtils;
import io.anserini.index.NotStoredException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractFeature implements Feature {
    private final IndexReader reader;

    public AbstractFeature(IndexReader reader) {
        this.reader = reader;
    }

    protected Map<String, Long> getDocumentVector(String docId) {
        try {
            return IndexReaderUtils.getDocumentVector(reader, docId);
        } catch (IOException| NotStoredException e) {
            throw new RuntimeException(e);
        }
    }

    protected long getDocumentLength(String docId) {
        return IndexReaderUtils.documentRaw(reader, docId).length();
    }

    protected double sum(double[] vector) {
        float s = 0;
        for (double e : vector) {
            s += e;
        }

        return s;
    }

    protected double[] multiply(double[] a, double[] b) {
        // element-wise multiply
        double[] result = new double[a.length];

        for (int i = 0; i < a.length; i++) {
            result[i] = a[i] * b[i];
        }

        return result;
    }

    protected int numDocuments() {
        return (int) IndexReaderUtils.getIndexStats(reader).get("documents");
    }

    protected long getTotalTermCount() {
        return (long) IndexReaderUtils.getIndexStats(reader).get("total_terms");
    }

    protected long getCollectionFrequency(String term) {
        try {
            Term t = new Term(IndexArgs.CONTENTS, term);
            return reader.totalTermFreq(t);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected IndexReader getReader() {
        return reader;
    }
}
