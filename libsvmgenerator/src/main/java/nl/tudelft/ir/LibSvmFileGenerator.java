package nl.tudelft.ir;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.index.IndexReaderUtils;
import org.apache.lucene.index.IndexReader;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LibSvmFileGenerator {

    public static void main(String[] args) throws IOException {
        IndexReader indexReader = IndexReaderUtils.getReader(args[0]);

        String dataDirectory = "/media/hd/ir-project/data";

        Path outputPath = Paths.get(dataDirectory, "libsvm_train.txt");
        Path queriesPath = Paths.get(dataDirectory, "msmarco-doctrain-queries.tsv");
        Path top100Path = Paths.get(dataDirectory, "msmarco-doctrain-top100");
        Path qrelsPath = Paths.get(dataDirectory, "msmarco-doctrain-qrels.tsv");

        LibSvmFileGenerator generator = new LibSvmFileGenerator(indexReader, queriesPath, top100Path, qrelsPath);
        generator.generate(outputPath);
    }

    private final IndexReader reader;
    private final Path queriesPath;
    private final Path top100Path;
    private final Path qrelsPath;

    private final List<Feature> features;

    public LibSvmFileGenerator(IndexReader reader, Path queriesPath, Path top100Path, Path qrelsPath) {
        this.reader = reader;
        this.queriesPath = queriesPath;
        this.top100Path = top100Path;
        this.qrelsPath = qrelsPath;

        this.features = Arrays.asList(
                new DocumentLengthFeature(reader)
        );
    }

    private static class Example {
        String queryId;
        String query;
        String docId;
        boolean isPositive;

        private Example(String queryId, String query, String docId, boolean isPositive) {
            this.queryId = queryId;
            this.query = query;
            this.docId = docId;
            this.isPositive = isPositive;
        }

        public static Example positive(String queryId, String query, String docId) {
            return new Example(queryId, query, docId, true);
        }

        public static Example negative(String queryId, String query, String docId) {
            return new Example(queryId, query, docId, false);
        }

        @Override
        public String toString() {
            return (isPositive ? "PositiveExample" : "NegativeExample") + "{" +
                    "queryId='" + queryId + '\'' +
                    ", query='" + query + '\'' +
                    ", docId='" + docId + '\'' +
                    '}';
        }
    }

    private static class LibSvmEntry {
        private boolean isPositive;
        private String queryId;
        private float[] features;

        public LibSvmEntry(boolean isPositive, String queryId, float[] features) {
            this.isPositive = isPositive;
            this.queryId = queryId;
            this.features = features;
        }

        public void write(StringBuilder builder) {
            builder.append(isPositive ? "1" : "0");
            builder.append(" ");
            builder.append("qid:");
            builder.append(queryId);

            for (int i = 0; i < features.length; i++) {
                builder.append(" ");
                builder.append(i);
                builder.append(":");
                builder.append(features[i]);
            }
            builder.append("\n");
        }
    }

    public void generate(Path outputPath) {
        System.out.println("Loading " + queriesPath + "...");
        Map<String, String> queries = loadQueries(queriesPath);

        // This query does not seem to have a top100 entry
        queries.remove("502557");

        System.out.println("Loading " + top100Path + "...");
        Map<String, List<String>> top100 = loadTop100(top100Path);

        System.out.println("Loading " + qrelsPath + "...");
        Map<String, String> qrels = loadQrels(qrelsPath);

        removePositiveExamplesFromTop100(qrels, top100);

        System.out.println("Generating positive and negative examples...");
        List<Example> examples = generatePositiveNegativeExamples(queries, qrels, top100).collect(Collectors.toList());

        System.out.print("Generating feature vectors... ");
        long start = System.currentTimeMillis();
        List<LibSvmEntry> entries = examples.stream()
                .map(this::generateLibSvmEntry)
                .collect(Collectors.toList());
        long duration = System.currentTimeMillis() - start;
        System.out.println("done in " + String.format("%.1f", (float)duration / 1000) + "s");

        System.out.println("Writing result to file...");
        StringBuilder output = new StringBuilder();
        entries.forEach(entry -> entry.write(output));

        try (FileWriter outputFile = new FileWriter(outputPath.toFile())){
            outputFile.write(output.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> loadQueries(Path path) {
        Map<String, String> queries = new HashMap<>();

        streamCsv(path, "\t").forEach(row -> {
            // qid query
            queries.put(row[0], row[1]);
        });

        return queries;
    }

    private Map<String, List<String>> loadTop100(Path path) {
        Map<String, List<String>> top100 = new HashMap<>();

        streamCsv(path, " ").forEach(row -> {
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

    private Map<String, String> loadQrels(Path path) {
        Map<String, String> qrels = new HashMap<>();

        streamCsv(path, " ").forEach(row -> {
            String qid = row[0];
            String docId = row[2];

            qrels.put(qid, docId);
        });

        return qrels;
    }

    private void removePositiveExamplesFromTop100(Map<String, String> qrels, Map<String, List<String>> top100) {
        for (String queryId : top100.keySet()) {
            String positiveDocId = qrels.get(queryId);
            top100.get(queryId).remove(positiveDocId);
        }
    }

    private Stream<Example> generatePositiveNegativeExamples(Map<String, String> queries, Map<String, String> qrels, Map<String, List<String>> top100) {
        return queries.entrySet().stream()
                .map(entry -> {
                    String queryId = entry.getKey();
                    String query = entry.getValue();

                    String positiveDocId = qrels.get(queryId);

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
                .flatMap(List::stream);
    }

    private Stream<String[]> streamCsv(Path path, String delimiter) {
        try {
            return Files.lines(path).map(line -> line.split(delimiter));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private float[] generateFeatureVector(Example example) {
        List<String> queryTerms = AnalyzerUtils.analyze(example.query);
        float[] featureVec = new float[features.size()];

        for (int i = 0; i < features.size(); i++) {
            featureVec[i] = features.get(i).score(queryTerms, example.docId);
        }

        return featureVec;
    }

    private LibSvmEntry generateLibSvmEntry(Example example) {
        float[] features = generateFeatureVector(example);

        return new LibSvmEntry(example.isPositive, example.queryId, features);
    }

    private Writer openFileWriter(Path path) {
        try {
            return new FileWriter(path.toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
