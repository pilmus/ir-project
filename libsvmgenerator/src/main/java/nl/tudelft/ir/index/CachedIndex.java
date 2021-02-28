package nl.tudelft.ir.index;

import io.anserini.index.IndexArgs;
import io.anserini.index.IndexReaderUtils;
import io.anserini.index.NotStoredException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CachedIndex implements Index {
    private final IndexReader reader;

    private final Map<String, Long> documentLengthCache = new ConcurrentHashMap<>();
    private final Map<String, Long> collectionFrequencyCache = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Integer> documentFrequencyCache = Collections.synchronizedMap(new HashMap<>());

    public CachedIndex(IndexReader reader) {
        this.reader = reader;
    }

    @Override
    public Map<String, Long> getDocumentVector(String docId) {
        try {
            return IndexReaderUtils.getDocumentVector(reader, docId);
        } catch (IOException| NotStoredException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getDocumentLength(String docId) {
        Long documentLength = documentLengthCache.get(docId);
        if (documentLength != null) {
            return documentLength;
        }

        documentLength = (long) IndexReaderUtils.documentRaw(reader, docId).length();

        documentLengthCache.put(docId, documentLength);

        return documentLength;
    }

    @Override
    public double getBm25Weight(String term, String docId, float k1, float b) {
        try {
            return IndexReaderUtils.getBM25AnalyzedTermWeightWithParameters(reader, docId, term, k1, b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getCollectionFrequency(String term) {
        Long frequency = collectionFrequencyCache.get(term);

        if (frequency != null) {
            return frequency;
        }

        try {
            frequency = reader.totalTermFreq(new Term(IndexArgs.CONTENTS, term));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        collectionFrequencyCache.put(term, frequency);

        return frequency;
    }

    @Override
    public int getDocumentFrequency(String term) {
        Integer frequency = documentFrequencyCache.get(term);

        if (frequency != null) {
            return frequency;
        }

        try {
            frequency = reader.docFreq(new Term(IndexArgs.CONTENTS, term));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        documentFrequencyCache.put(term, frequency);

        return frequency;
    }

    @Override
    public int getCollectionSize() {
        return reader.numDocs();
    }

    @Override
    public long getTotalTermCount() {
        try {
            return reader.getSumTotalTermFreq(IndexArgs.CONTENTS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
