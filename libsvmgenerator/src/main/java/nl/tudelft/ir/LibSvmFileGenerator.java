package nl.tudelft.ir;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.index.IndexReaderUtils;
import nl.tudelft.ir.feature.*;
import nl.tudelft.ir.index.Collection;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.Index;
import nl.tudelft.ir.index.LuceneIndex;
import org.apache.lucene.index.IndexReader;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LibSvmFileGenerator {

    public static void main(String[] args) throws IOException {
        IndexReader indexReader = IndexReaderUtils.getReader(args[0]);

        String modus = "test";
        String size = "";

        String dataDirectory = "data";

        Path outputPath = Paths.get(dataDirectory, String.format("libsvm_%s%s.txt", modus, size));
        Path queriesPath = Paths.get(dataDirectory, String.format("msmarco-doc%s-queries%s.tsv", modus, size));
        Path top100Path = Paths.get(dataDirectory, String.format("msmarco-doc%s-top100.tsv", modus));
        Path qrelsPath = Paths.get(dataDirectory, String.format("msmarco-doc%s-qrels.tsv", modus));

        Index index = new LuceneIndex(indexReader);

        List<Feature> features = Arrays.asList(
                /* feature index */
                /* 0             */ new DocumentLengthFeature(),
                /* 1             */ new TfFeature(),
                /* 2             */ new IdfFeature(),
                /* 3             */ new TfIdfFeature(),
                /* 4             */ new Bm25Feature(index, 1.2f, 0.75f),
                /* 5             */ new LmirFeature(new LmirFeature.JelinekMercerSmoothing(0.1)),
                /* 6             */ new LmirFeature(new LmirFeature.DirichletPriorSmoothing(2000)),
                /* 7             */ new LmirFeature(new LmirFeature.AbsoluteDiscountingSmoothing(0.7))
        );

        LibSvmFileGenerator generator = new LibSvmFileGenerator(index, queriesPath, top100Path, qrelsPath, features);
        if (modus.equals("test")) {
            generator.generateFromTop100(outputPath);
        } else {
            generator.generatePositiveNegative(outputPath);
        }
    }

    private final Index index;
    private final Path queriesPath;
    private final Path top100Path;
    private final Path qrelsPath;
    private final List<Feature> features;

    public LibSvmFileGenerator(Index index, Path queriesPath, Path top100Path, Path qrelsPath, List<Feature> features) {
        this.index = index;
        this.queriesPath = queriesPath;
        this.top100Path = top100Path;
        this.qrelsPath = qrelsPath;
        this.features = features;
    }

    public void generatePositiveNegative(Path outputPath) {
        long start = System.currentTimeMillis();

        System.out.println("Loading " + queriesPath + "...");
        Map<String, String> queries = loadQueries(queriesPath);

        // This query does not seem to have a top100 entry
        queries.remove("502557");

        System.out.println("Loading " + top100Path + "...");
        Map<String, List<String>> top100 = loadTop100(top100Path);

        System.out.println("Loading " + qrelsPath + "...");
        Map<String, QRel> qrels = loadQrels(qrelsPath);

        removePositiveExamplesFromTop100(qrels, top100);

        System.out.println("Generating positive and negative examples...");
        List<Example> examples = generatePositiveNegativeExamples(queries, qrels, top100);

        System.out.println("Generating feature vectors... ");

        AtomicInteger doneCounter = new AtomicInteger(0);
        Thread progressThread = new Thread(new ProgressIndicator(doneCounter, examples.size()));
        progressThread.start();

        Collection collection = new Collection(index);

        List<LibSvmEntry> entries = examples.stream()
                .parallel()
                .map(example -> {
                    LibSvmEntry entry = generateLibSvmEntry(example, collection);
                    doneCounter.incrementAndGet();

                    return entry;
                })
                .collect(Collectors.toList());

        try {
            progressThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

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

    public void generateFromTop100(Path outputPath) {
        long start = System.currentTimeMillis();

        System.out.println("Loading " + queriesPath + "...");
        Map<String, String> queries = loadQueries(queriesPath);

        // This query does not seem to have a top100 entry
        queries.remove("502557");

        System.out.println("Loading " + top100Path + "...");
        Map<String, List<String>> top100 = loadTop100(top100Path);

        System.out.println("Generating top100 examples...");
        List<Example> examples = generateTop100Examples(queries, top100);

        System.out.println("Generating feature vectors... ");

        AtomicInteger doneCounter = new AtomicInteger(0);
        Thread progressThread = new Thread(new ProgressIndicator(doneCounter, examples.size()));
        progressThread.start();

        Collection collection = new Collection(index);

        List<LibSvmEntry> entries = examples.stream()
                .parallel()
                .map(example -> {
                    LibSvmEntry entry = generateLibSvmEntry(example, collection);
                    doneCounter.incrementAndGet();

                    return entry;
                })
                .collect(Collectors.toList());

        try {
            progressThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

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

    private Map<String, String> loadQueries(Path path) {
        Map<String, String> queries = new HashMap<>();

        readCsv(path, "\t").forEach(row -> {
            // qid query
            queries.put(row[0], row[1]);
        });

        return queries;
    }

    private Map<String, List<String>> loadTop100(Path path) {
        Map<String, List<String>> top100 = new HashMap<>();

        readCsv(path, " ").forEach(row -> {
            String qid = row[0];
            String docId = row[2];

            if (top100.containsKey(qid)) {
                top100.get(qid).add(docId);
            } else {
                top100.put(qid, new ArrayList<>(Collections.singleton(docId)));
            }
        });

        return top100;
    }

    private Map<String, QRel> loadQrels(Path path) {
        Map<String, QRel> qrels = new HashMap<>();

        readCsv(path, " ").forEach(row -> {
            String qid = row[0];
            String docId = row[2];
            String label = row[3];

            qrels.put(qid, new QRel(qid, docId, label));
        });

        return qrels;
    }

    private void removePositiveExamplesFromTop100(Map<String, QRel> qrels, Map<String, List<String>> top100) {
        for (String queryId : top100.keySet()) {
            String positiveDocId = qrels.get(queryId).docId;
            top100.get(queryId).remove(positiveDocId);
        }
    }

    private List<Example> generatePositiveNegativeExamples(Map<String, String> queries, Map<String, QRel> qrels, Map<String, List<String>> top100) {
        return queries.entrySet().stream()
                .map(entry -> {
                    String queryId = entry.getKey();
                    String query = entry.getValue();

                    String positiveDocId = qrels.get(queryId).docId;

                    List<String> allNegativeDocIds = top100.get(queryId);
                    if (allNegativeDocIds == null) {
                        throw new RuntimeException("No top100 entry found for query " + queryId);
                    }

                    String negativeDocId = allNegativeDocIds.get(ThreadLocalRandom.current().nextInt(allNegativeDocIds.size()));

                    return Arrays.asList(
                            Example.positive(queryId, query, positiveDocId),
                            Example.negative(queryId, query, negativeDocId)
                    );
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private List<Example> generateQRelExamples(Map<String, String> queries, Map<String, QRel> qrels) {
        return qrels.entrySet().stream()
                .map(entry -> {
                    String queryId = entry.getKey();
                    QRel qrel = entry.getValue();

                    String docId = qrel.docId;
                    String label = qrel.label;

                    String query = queries.get(queryId);

                    return new Example(queryId, query, docId, label);
                })
                .collect(Collectors.toList());

    }

    private List<Example> generateTop100Examples(Map<String, String> queries, Map<String, List<String>> top100) {
        return top100.entrySet().stream()
                .flatMap(entry -> {
                    String queryId = entry.getKey();
                    String query = queries.get(queryId);

                    List<String> docIds = entry.getValue();

                    return docIds.stream()
                            .map(docId -> new Example(queryId, query, docId, "0"));
                })
                .collect(Collectors.toList());
    }

    private Document retrieveDocument(String id) {
        Map<String, Long> documentVector = index.getDocumentVector(id);

        return new Document(id, documentVector, index.getDocumentLength(id));
    }

    private Stream<String[]> readCsv(Path path, String delimiter) {
        try {
            return Files.lines(path).map(line -> line.split(delimiter));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private double[] generateFeatureVector(Example example, Collection collection) {
        List<String> queryTerms = AnalyzerUtils.analyze(example.query);
        double[] featureVec = new double[features.size()];

        Document document = retrieveDocument(example.docId);

        for (int i = 0; i < features.size(); i++) {
            featureVec[i] = features.get(i).score(queryTerms, document, collection);
        }

        return featureVec;
    }

    private LibSvmEntry generateLibSvmEntry(Example example, Collection collection) {
        double[] features = generateFeatureVector(example, collection);

        return new LibSvmEntry(example.label, example.queryId, features, example.docId);
    }

    private static class QRel {
        String qid;
        String docId;
        String label;

        private QRel(String qid, String docId, String label) {
            this.qid = qid;
            this.docId = docId;
            this.label = label;
        }
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
        private final double[] features;
        private final String docId;

        public LibSvmEntry(String label, String queryId, double[] features, String docId) {
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

    private static class ProgressIndicator implements Runnable {
        private final static long UPDATE_INTERVAL = 500;

        private final AtomicInteger counter;
        private final int total;

        public ProgressIndicator(AtomicInteger counter, int total) {
            this.counter = counter;
            this.total = total;
        }

        @Override
        public void run() {
            int lastCount = 0;
            while (lastCount < total) {
                int count = counter.get();
                int difference = count - lastCount;
                double iterationsPerSec = difference * (1000.0 / (double) UPDATE_INTERVAL);
                String percentage = String.format("%.2f", ((double) count / total) * 100);
                System.out.println(percentage + "% " + count + "/" + total + " [" + String.format("%.2f", iterationsPerSec) + " it/s]");

                lastCount = count;

                try {
                    Thread.sleep(UPDATE_INTERVAL);
                } catch (InterruptedException e) {
                    break;
                }
            }

            System.out.println();
        }
    }
}
