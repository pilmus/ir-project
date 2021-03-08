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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LibSvmFileGenerator {

    public static void main(String[] args) throws IOException {
        IndexReader indexReader = IndexReaderUtils.getReader(args[0]);

        String modus = "dev";
        String size = "";
        long maxNumTriples = 1_000_000;
        int negativeDocsPerPositiveDoc = 100;

        String dataDirectory = "data";

        Path outputPath = Paths.get(dataDirectory, String.format("libsvm_%s%s.txt", modus, size));
        Path queriesPath = Paths.get(dataDirectory, String.format("msmarco-doc%s-queries%s.tsv", modus, size));
        Path top100Path = Paths.get(dataDirectory, String.format("msmarco-doc%s-top100.tsv", modus));
        Path qrelsPath = Paths.get(dataDirectory, String.format("msmarco-doc%s-qrels.tsv", modus));

        Index index = new LuceneIndex(indexReader);

        List<Feature> features = Features.LAMBDAMART_DEFAULT_FEATURES;

        LibSvmFileGenerator generator = new LibSvmFileGenerator(index, queriesPath, top100Path, qrelsPath, features);
        generator.generateFromTop100(maxNumTriples, outputPath, negativeDocsPerPositiveDoc);
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

    public void generateFromTop100(long maxNumTriples, Path outputPath, int negativeDocsPerPositiveDoc) {
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

        System.out.println("Generating triples...");
        List<Example> examples = generateTriples(queries, top100, qrels, negativeDocsPerPositiveDoc)
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

    private void removePositiveExamplesFromTop100(Map<String, QRel> qrels, Map<String, List<String>> top100) {
        for (String queryId : top100.keySet()) {
            String positiveDocId = qrels.get(queryId).getDocId();
            top100.get(queryId).remove(positiveDocId);
        }
    }

    private Stream<Example> generateTriples(Map<String, String> queries, Map<String, List<String>> top100, Map<String, QRel> qrels, int negativeDocsPerPositiveDoc) {
        List<String> allDocumentIds = top100.values().stream().flatMap(Collection::stream).distinct().collect(Collectors.toList());

        return queries.entrySet().stream()
                .flatMap(entry -> {
                    String queryId = entry.getKey();
                    String query = entry.getValue();

                    // Grab random non-relevant documents from top-100
                    // We define a non-relevant document to be a document that occurs
                    // in the top-100 of a *different* query
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    List<String> negativeDocIds = random.ints(0, allDocumentIds.size())
                            .limit(negativeDocsPerPositiveDoc)
                            .mapToObj(allDocumentIds::get)
                            .collect(Collectors.toList());

                    QRel qrel = qrels.get(queryId);
                    if (!qrel.getLabel().equals("1")) {
                        throw new RuntimeException("QRel unexpectedly has label " + qrel.getLabel() + " (expected \"1\")");
                    }

                    String positiveDocId = qrel.getDocId();

                    List<Example> examples = new ArrayList<>();
                    for (String negativeDocId : negativeDocIds) {
                        if (negativeDocId.equals(positiveDocId)) {
                            // theoretically possible, but a tiny chance
                            continue;
                        }

                        examples.add(Example.positive(queryId, query, positiveDocId));
                        examples.add(Example.negative(queryId, query, negativeDocId));
                    }

                    return examples.stream();
                });
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
