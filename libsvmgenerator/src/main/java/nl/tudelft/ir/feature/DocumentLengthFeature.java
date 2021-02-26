package nl.tudelft.ir.feature;

import io.anserini.index.IndexReaderUtils;
import org.apache.lucene.index.IndexReader;

import java.util.List;

public class DocumentLengthFeature implements Feature {
    private final IndexReader reader;

    public DocumentLengthFeature(IndexReader reader) {
        this.reader = reader;
    }

    @Override
    public double score(List<String> queryTerms, String docId) {
        return IndexReaderUtils.documentRaw(reader, docId).length();
    }
}
