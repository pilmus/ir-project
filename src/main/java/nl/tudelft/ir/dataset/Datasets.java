package nl.tudelft.ir.dataset;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

abstract public class Datasets {

    private Datasets() {
    }


    public static Map<String, List<String>> loadTop100(Path path) {
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

    public static Map<String, String> loadQueries(Path path) {
        Map<String, String> queries = new HashMap<>();

        readCsv(path, "\t").forEach(row -> {
            // qid query
            queries.put(row[0], row[1]);
        });

        return queries;
    }

    public static Map<String, QRel> loadQrels(Path path) {
        Map<String, QRel> qrels = new HashMap<>();

        readCsv(path, " ").forEach(row -> {
            String qid = row[0];
            String docId = row[2];
            String label = row[3];

            qrels.put(qid, new QRel(qid, docId, label));
        });

        return qrels;
    }

    private static Stream<String[]> readCsv(Path path, String delimiter) {
        try {
            return Files.lines(path).map(line -> line.split(delimiter));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
