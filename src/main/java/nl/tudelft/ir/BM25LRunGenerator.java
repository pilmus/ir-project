package nl.tudelft.ir;

import io.anserini.analysis.DefaultEnglishAnalyzer;
import io.anserini.index.IndexArgs;
import io.anserini.index.IndexReaderUtils;
import io.anserini.search.query.BagOfWordsQueryGenerator;
import io.anserini.search.query.QueryGenerator;
import nl.tudelft.ir.dataset.Datasets;
import nl.tudelft.ir.util.ProgressIndicator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BM25LRunGenerator {

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        IndexReader indexReader = IndexReaderUtils.getReader(args[0]);

        String runId = "BM25L";
        String dataDirectory = "data";
        String runDirectory = "runs";

        Path outputPath = Paths.get(runDirectory, "run.bm25l.k1.4.68.b.0.87.d.0.5.expanded.txt");
        Path queriesPath = Paths.get(dataDirectory, "msmarco-doctest-queries.tsv");

        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryGenerator queryGenerator = new BagOfWordsQueryGenerator();
        Analyzer analyzer = DefaultEnglishAnalyzer.newDefaultInstance();

        searcher.setSimilarity(new BM25LSimilarity(4.68f, 0.87f, 0.5f));

        System.out.println("Loading " + queriesPath + "...");
        Map<String, String> queries = Datasets.loadQueries(queriesPath);

        AtomicInteger doneCounter = new AtomicInteger(0);
        Thread progressThread = new Thread(new ProgressIndicator(doneCounter, queries.size()));
        progressThread.start();

        System.out.println("Executing queries...");
        List<SearchResult> searchResults = queries.entrySet().stream()
                .parallel()
                .flatMap((entry) -> {
                    String queryId = entry.getKey();
                    String queryString = entry.getValue();

                    Query query = queryGenerator.buildQuery(IndexArgs.CONTENTS, analyzer, queryString);

                    TopDocs rs = search(searcher, query);

                    Stream<SearchResult> searchResultStream = Arrays.stream(rs.scoreDocs)
                            .map(scoreDoc -> {
                                int luceneDocId = scoreDoc.doc;
                                String docId = IndexReaderUtils.convertLuceneDocidToDocid(indexReader, luceneDocId);
                                return new SearchResult(queryId, docId, ArrayUtils.indexOf(rs.scoreDocs, scoreDoc) + 1, scoreDoc.score);
                            });


                    doneCounter.incrementAndGet();
                    return searchResultStream;
                })
                .collect(Collectors.toList());

        progressThread.interrupt();

        System.out.println("Writing result to file...");
        StringBuilder output = new StringBuilder();
        searchResults.forEach(entry -> entry.write(output, runId));

        try (FileWriter outputFile = new FileWriter(outputPath.toFile())) {
            outputFile.write(output.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("Done in " + String.format("%.1f", (float) duration / 1000) + "s");
    }

    private static TopDocs search(IndexSearcher searcher, Query query) {
        try {
            return searcher.search(query, 100);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class SearchResult {
        private final String queryId;
        private final String documentId;
        private final int rank;
        private final float score;

        public SearchResult(String queryId, String documentId, int rank, float score) {
            this.queryId = queryId;
            this.documentId = documentId;
            this.rank = rank;
            this.score = score;
        }

        public void write(StringBuilder builder, String runId) {
            builder.append(queryId);
            builder.append(" Q0 ");
            builder.append(documentId);
            builder.append(" ");
            builder.append(rank);
            builder.append(" ");
            builder.append(score);
            builder.append(" ");
            builder.append(runId);
            builder.append("\n");
        }
    }

}
