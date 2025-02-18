package nl.tudelft.ir;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.analysis.DefaultEnglishAnalyzer;
import io.anserini.index.IndexArgs;
import io.anserini.index.IndexReaderUtils;
import io.anserini.search.query.BagOfWordsQueryGenerator;
import io.anserini.search.query.QueryGenerator;
import nl.tudelft.ir.dataset.Datasets;
import nl.tudelft.ir.feature.Features;
import nl.tudelft.ir.index.DocumentCollection;
import nl.tudelft.ir.index.Index;
import nl.tudelft.ir.index.LuceneIndex;
import nl.tudelft.ir.util.ProgressIndicator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RerankRunGenerator {

    public static void main(String[] args) throws IOException {
        IndexReader indexReader = IndexReaderUtils.getReader(args[0]);
        String modelPath = args[1];

        String dataDirectory = "data";
        String runDirectory = "runs";

        Path outputPath = Paths.get(runDirectory, "bm25.lambdamart.top100.txt");
        Path queriesPath = Paths.get(dataDirectory, "msmarco-doctest-queries.tsv");

        Similarity firstPhaseSimilarity = new BM25LSimilarity(4.68f, 0.87f, 0.5f);

        doRun(indexReader, firstPhaseSimilarity, modelPath, outputPath, queriesPath);
    }

    private static void doRun(IndexReader indexReader, Similarity similarity, String lambdaMartModelPath, Path outputPath, Path queriesPath) {
        long start = System.currentTimeMillis();

        Index index = new LuceneIndex(indexReader);
        DocumentCollection documentCollection = new DocumentCollection(index);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        QueryGenerator queryGenerator = new BagOfWordsQueryGenerator();
        Analyzer analyzer = DefaultEnglishAnalyzer.newDefaultInstance();

        LambdaMARTReranker reranker = new LambdaMARTReranker(Features.LAMBDAMART_DEFAULT_FEATURES, index, documentCollection, indexReader, lambdaMartModelPath);

        searcher.setSimilarity(similarity);

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
                    List<String> queryTokens = AnalyzerUtils.analyze(analyzer, queryString);

                    Query query = queryGenerator.buildQuery(IndexArgs.CONTENTS, analyzer, queryString);

                    TopDocs rs = search(searcher, query);
                    List<LambdaMARTReranker.RankedDocument> reranked = reranker.rerankTopDocs(rs, queryTokens);

                    Stream<SearchResult> searchResultStream = reranked.stream()
                            .map(rankedDocument -> new SearchResult(queryId, rankedDocument.getDocument().getId(), rankedDocument.getRank(), rankedDocument.getScore()));

                    doneCounter.incrementAndGet();
                    return searchResultStream;
                })
                .collect(Collectors.toList());

        try {
            progressThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Writing result to file...");
        StringBuilder output = new StringBuilder();
        searchResults.forEach(entry -> entry.write(output, "BM25L_LambdaMART"));

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
