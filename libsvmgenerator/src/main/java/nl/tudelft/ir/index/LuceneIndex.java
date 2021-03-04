package nl.tudelft.ir.index;

import io.anserini.index.IndexArgs;
import io.anserini.index.IndexReaderUtils;
import io.anserini.index.NotStoredException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class LuceneIndex implements Index {
    private final IndexReader reader;

    public LuceneIndex(IndexReader reader) {
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
        try {
            String raw = reader.document(IndexReaderUtils.convertDocidToLuceneDocid(reader, docId), Set.of(IndexArgs.RAW))
                    .get(IndexArgs.RAW);

            return raw.length();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        try {
            return reader.totalTermFreq(new Term(IndexArgs.CONTENTS, term));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getDocumentFrequency(String term) {
        try {
            return reader.docFreq(new Term(IndexArgs.CONTENTS, term));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getNumDocuments() {
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

    @Override
    public double getAverageDocumentLength() {
        return getTotalTermCount() / (double) getNumDocuments();
    }

    @Override
    public Document retrieveById(String docId) {
        Map<String, Long> documentVector = getDocumentVector(docId);

        return new Document(docId, documentVector, getDocumentLength(docId));
    }
}
