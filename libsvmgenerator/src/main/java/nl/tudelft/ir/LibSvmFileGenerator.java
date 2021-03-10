package nl.tudelft.ir;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.analysis.DefaultEnglishAnalyzer;
import io.anserini.index.IndexArgs;
import io.anserini.index.IndexReaderUtils;
import io.anserini.search.query.BagOfWordsQueryGenerator;
import io.anserini.search.query.QueryGenerator;
import nl.tudelft.ir.dataset.Datasets;
import nl.tudelft.ir.dataset.QRel;
import nl.tudelft.ir.feature.Feature;
import nl.tudelft.ir.feature.Features;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;
import nl.tudelft.ir.index.Index;
import nl.tudelft.ir.index.LuceneIndex;
import nl.tudelft.ir.util.ProgressIndicator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LibSvmFileGenerator {

    private final IndexReader indexReader;

    private final Index index;

    public LibSvmFileGenerator(Index index, IndexReader indexReader, Path queriesPath, Path top100Path, Path qrelsPath, List<Feature> features) {
        this.index = index;
        this.indexReader = indexReader;
        this.queriesPath = queriesPath;
        this.top100Path = top100Path;
        this.qrelsPath = qrelsPath;
        this.features = features;
    }

    private final Path queriesPath;
    private final Path top100Path;
    private final Path qrelsPath;
    private final List<Feature> features;

    public static void main(String[] args) throws IOException {
        IndexReader indexReader = IndexReaderUtils.getReader(args[0]);

        String modus = "dev";
        String size = "";
        long maxNumTriples = 1_000_000;
        int negativeDocsPerPositiveDoc = 2;

        String dataDirectory = "data";

        Path outputPath = Paths.get(dataDirectory, String.format("libsvm_%s%s.txt", modus, size));
        Path queriesPath = Paths.get(dataDirectory, String.format("msmarco-doc%s-queries%s.tsv", modus, size));
        Path top100Path = Paths.get(dataDirectory, String.format("msmarco-doc%s-top100.tsv", modus));
        Path qrelsPath = Paths.get(dataDirectory, String.format("msmarco-doc%s-qrels.tsv", modus));

        Index index = new LuceneIndex(indexReader);

        List<Feature> features = Features.LAMBDAMART_DEFAULT_FEATURES;

        LibSvmFileGenerator generator = new LibSvmFileGenerator(index, indexReader, queriesPath, top100Path, qrelsPath, features);
        generator.generateFromTop100(maxNumTriples, outputPath, negativeDocsPerPositiveDoc);
    }

    private static TopDocs search(IndexSearcher searcher, Query query, int n) {
        try {
            return searcher.search(query, n);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void generateFromTop100(long maxNumTriples, Path outputPath, int negativeDocsPerPositiveDoc) {
        long start = System.currentTimeMillis();

        System.out.println("Loading " + queriesPath + "...");
        Map<String, String> queries = Datasets.loadQueries(queriesPath);

        System.out.println("Loading " + qrelsPath + "...");
        Map<String, QRel> qrels = Datasets.loadQrels(qrelsPath);

        System.out.println("Generating triples...");
        List<Example> examples = generateTriples(queries, qrels, negativeDocsPerPositiveDoc)
                .limit(maxNumTriples)
                .collect(Collectors.toList());

        System.out.println("Generating feature vectors... ");
        AtomicInteger doneCounter = new AtomicInteger(0);
        Thread progressThread = new Thread(new ProgressIndicator(doneCounter, examples.size()));
        progressThread.start();

        DocumentCollection documentCollection = new DocumentCollection(index);

        List<LibSvmEntry> entries = examples.stream()
                .parallel()
                .map(example -> {
                    LibSvmEntry entry = generateLibSvmEntry(example, documentCollection);
                    doneCounter.incrementAndGet();

                    return entry;
                })
                .collect(Collectors.toList());

        progressThread.interrupt();

        System.out.println("Writing result to file...");
        StringBuilder output = new StringBuilder();
        entries.forEach(entry -> entry.write(output));

        try (FileWriter outputFile = new FileWriter(outputPath.toFile())) {
            outputFile.write(output.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("Done in " + String.format("%.1f", (float) duration / 1000) + "s");
    }

    private LibSvmEntry generateLibSvmEntry(Example example, DocumentCollection documentCollection) {
        List<String> queryTerms = AnalyzerUtils.analyze(example.query);
        Document document = documentCollection.find(example.docId);

        float[] featuresVec = Features.generateVector(features, queryTerms, document, documentCollection);

        return new LibSvmEntry(example.label, example.queryId, featuresVec, example.docId);
    }

    private Stream<Example> generateTriples(Map<String, String> queries, Map<String, QRel> qrels, int negativeDocsPerPositiveDoc) {
        IndexSearcher searcher = new IndexSearcher(indexReader);
        searcher.setSimilarity(new BM25Similarity());

        return qrels.entrySet().stream()
                .limit(500_000)
                .parallel()
                .flatMap(entry -> {
                    QRel qrel = entry.getValue();
                    String queryId = qrel.getQid();
                    String queryString = queries.get(queryId);

                    if (queryString == null) {
                        throw new RuntimeException("Query " + queryId + " does not exist");
                    }

                    Analyzer analyzer = DefaultEnglishAnalyzer.newDefaultInstance();
                    QueryGenerator queryGenerator = new BagOfWordsQueryGenerator();
                    Query query = queryGenerator.buildQuery(IndexArgs.CONTENTS, analyzer, queryString);

                    TopDocs rs = search(searcher, query, negativeDocsPerPositiveDoc);

                    List<String> bm25DocumentIds = Arrays.stream(rs.scoreDocs)
                            .map(scoreDoc -> IndexReaderUtils.convertLuceneDocidToDocid(indexReader, scoreDoc.doc))
                            .distinct()
                            .collect(Collectors.toList());

                    String positiveDocId = qrel.getDocId();

                    bm25DocumentIds.remove(positiveDocId);

                    List<Example> examples = new ArrayList<>();
                    examples.add(Example.positive(queryId, queryString, positiveDocId));

                    for (String negativeDocId : bm25DocumentIds) {
                        examples.add(Example.negative(queryId, queryString, negativeDocId));
                    }

                    return examples.stream();
                });
    }

    private static class Example {
        String queryId;
        String query;
        String docId;
        String label;

        private Example(String queryId, String query, String docId, String label) {
            this.queryId = queryId;
            this.query = query;
            this.docId = docId;
            this.label = label;
        }

        public static Example positive(String queryId, String query, String docId) {
            return new Example(queryId, query, docId, "1");
        }

        public static Example negative(String queryId, String query, String docId) {
            return new Example(queryId, query, docId, "0");
        }

        @Override
        public String toString() {
            return ("Example" + "{" +
                    "queryId='" + queryId + '\'' +
                    ", query='" + query + '\'' +
                    ", docId='" + docId + '\'' +
                    "label='" + label + '\'' +
                    '}');
        }
    }

    private static class LibSvmEntry {
        private final String label;
        private final String queryId;
        private final float[] features;
        private final String docId;

        public LibSvmEntry(String label, String queryId, float[] features, String docId) {
            this.label = label;
            this.queryId = queryId;
            this.features = features;
            this.docId = docId;
        }

        public void write(StringBuilder builder) {
            builder.append(label);
            builder.append(" ");
            builder.append("qid:");
            builder.append(queryId);

            for (int i = 0; i < features.length; i++) {
                builder.append(" ");
                builder.append(i + 1);
                builder.append(":");
                builder.append(features[i]);
            }

            builder.append(" # ");
            builder.append(docId);

            builder.append("\n");
        }
    }

}
