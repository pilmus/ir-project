package nl.tudelft.ir.index;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.index.IndexArgs;
import io.anserini.index.IndexReaderUtils;
import io.anserini.index.NotStoredException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
        } catch (IOException | NotStoredException e) {
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

        String contents = getDocumentContents(docId);
        // first line contains "<TEXT>"
        // second line is url
        // third line is title
        // the rest is body
        // ends with "</TEXT>"
        int firstNewline = contents.indexOf('\n');
        int secondNewline = contents.indexOf('\n', firstNewline + 1);
        int thirdNewline = contents.indexOf('\n', secondNewline + 1);

        String url = contents.substring(firstNewline + 1, secondNewline);
        String title = contents.substring(secondNewline + 1, thirdNewline);
        // right strip "</TEXT>"
        String body = contents.substring(thirdNewline + 1, contents.length() - "</TEXT>\n".length());

        return new Document(
                docId,
                new Document.Field(contents, documentVector),
                createFieldFromRawString(title),
                createFieldFromRawString(url),
                createFieldFromRawString(body)
        );
    }

    private Document.Field createFieldFromRawString(String raw) {
        List<String> terms = AnalyzerUtils.analyze(raw);

        Map<String, Long> vector = new HashMap<>();
        for (String term : terms) {
            vector.put(term, vector.getOrDefault(term, 0L) + 1);
        }

        return new Document.Field(raw, vector);
    }

    private String getDocumentContents(String docId) {
        try {
            return reader.document(IndexReaderUtils.convertDocidToLuceneDocid(reader, docId), Set.of(IndexArgs.RAW))
                    .get(IndexArgs.RAW);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
