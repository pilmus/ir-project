package nl.tudelft.ir;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.index.IndexReaderUtils;
import nl.tudelft.ir.dataset.Datasets;
import nl.tudelft.ir.dataset.QRel;
import nl.tudelft.ir.feature.Feature;
import nl.tudelft.ir.feature.Features;
import nl.tudelft.ir.index.Document;
import nl.tudelft.ir.index.DocumentCollection;
import nl.tudelft.ir.index.Index;
import nl.tudelft.ir.index.LuceneIndex;
import nl.tudelft.ir.util.ProgressIndicator;
import org.apache.lucene.index.IndexReader;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LibSvmFileGenerator {

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

        List<Feature> features = Features.LAMBDAMART_DEFAULT_FEATURES;

        LibSvmFileGenerator generator = new LibSvmFileGenerator(index, queriesPath, top100Path, qrelsPath, features);
        if (modus.equals("test")) {
            generator.generateFromTop100(outputPath);
        } else {
            generator.generatePositiveNegative(outputPath);
        }
    }

    public void generatePositiveNegative(Path outputPath) {
        long start = System.currentTimeMillis();

        System.out.println("Loading " + queriesPath + "...");
        Map<String, String> queries = Datasets.loadQueries(queriesPath);

        // This query does not seem to have a top100 entry
        queries.remove("502557");

        System.out.println("Loading " + top100Path + "...");
        Map<String, List<String>> top100 = Datasets.loadTop100(top100Path);

        System.out.println("Loading " + qrelsPath + "...");
        Map<String, QRel> qrels = Datasets.loadQrels(qrelsPath);

        removePositiveExamplesFromTop100(qrels, top100);

        System.out.println("Generating positive and negative examples...");
        List<Example> examples = generatePositiveNegativeExamples(queries, qrels, top100);

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
        Map<String, String> queries = Datasets.loadQueries(queriesPath);

        // This query does not seem to have a top100 entry
        queries.remove("502557");

        System.out.println("Loading " + top100Path + "...");
        Map<String, List<String>> top100 = Datasets.loadTop100(top100Path);

        System.out.println("Generating top100 examples...");
        List<Example> examples = generateTop100Examples(queries, top100);

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

    private void removePositiveExamplesFromTop100(Map<String, QRel> qrels, Map<String, List<String>> top100) {
        for (String queryId : top100.keySet()) {
            String positiveDocId = qrels.get(queryId).getDocId();
            top100.get(queryId).remove(positiveDocId);
        }
    }

    private List<Example> generatePositiveNegativeExamples(Map<String, String> queries, Map<String, QRel> qrels, Map<String, List<String>> top100) {
        return queries.entrySet().stream()
                .flatMap(entry -> {
                    String queryId = entry.getKey();
                    String query = entry.getValue();

                    String positiveDocId = qrels.get(queryId).getDocId();

                    List<String> allNegativeDocIds = top100.get(queryId);
                    if (allNegativeDocIds == null) {
                        throw new RuntimeException("No top100 entry found for query " + queryId);
                    }

                    String negativeDocId = allNegativeDocIds.get(ThreadLocalRandom.current().nextInt(allNegativeDocIds.size()));

                    return Stream.of(
                            Example.positive(queryId, query, positiveDocId),
                            Example.negative(queryId, query, negativeDocId)
                    );
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

    private LibSvmEntry generateLibSvmEntry(Example example, DocumentCollection documentCollection) {
        List<String> queryTerms = AnalyzerUtils.analyze(example.query);
        Document document = documentCollection.find(example.docId);

        float[] featuresVec = Features.generateVector(features, queryTerms, document, documentCollection);

        return new LibSvmEntry(example.label, example.queryId, featuresVec, example.docId);
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
