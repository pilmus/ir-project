/*
 * Heavily modified on io.anserini.search.SearchCollection.
 */
package nl.tudelft.ir;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.analysis.DefaultEnglishAnalyzer;
import io.anserini.index.IndexArgs;
import io.anserini.rerank.RerankerCascade;
import io.anserini.rerank.RerankerContext;
import io.anserini.rerank.ScoredDocuments;
import io.anserini.rerank.lib.ScoreTiesAdjusterReranker;
import io.anserini.search.SearchArgs;
import io.anserini.search.query.BagOfWordsQueryGenerator;
import io.anserini.search.query.QueryGenerator;
import io.anserini.search.similarity.TaggedSimilarity;
import io.anserini.search.topicreader.TsvIntTopicReader;
import nl.tudelft.ir.feature.Features;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.MMapDirectory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class SearchCollectionWithBm25AndLambdaMart implements Closeable {

    private static final Logger LOG = LogManager.getLogger(io.anserini.search.SearchCollection.class);
    private final SearchArgs args;
    private final IndexReader reader;
    private final Analyzer analyzer;

    public SearchCollectionWithBm25AndLambdaMart(SearchArgs args) throws IOException {
        this.args = args;
        Path indexPath = Paths.get(args.index);

        if (!Files.exists(indexPath) || !Files.isDirectory(indexPath) || !Files.isReadable(indexPath)) {
            throw new IllegalArgumentException(String.format("Index path '%s' does not exist or is not a directory.", args.index));
        }

        LOG.info("============ Initializing Searcher ============");
        LOG.info("Index: " + indexPath);
        this.reader = DirectoryReader.open(MMapDirectory.open(indexPath));

        // Default to English
        analyzer = DefaultEnglishAnalyzer.fromArguments(args.stemmer, args.keepstop, args.stopwords);
        LOG.info("Language: en");
        LOG.info("Stemmer: " + args.stemmer);
        LOG.info("Keep stopwords? " + args.keepstop);
        LOG.info("Stopwords file " + args.stopwords);
    }

    public static void main(String[] args) throws Exception {
        SearchArgs searchArgs = new SearchArgs();
        CmdLineParser parser = new CmdLineParser(searchArgs, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.err.println("Example: SearchCollection" + parser.printExample(OptionHandlerFilter.REQUIRED));
            return;
        }

        final long start = System.nanoTime();
        SearchCollectionWithBm25AndLambdaMart searcher;

        // We're at top-level already inside a main; makes no sense to propagate exceptions further, so reformat the
        // except messages and display on console.
        try {
            searcher = new SearchCollectionWithBm25AndLambdaMart(searchArgs);
        } catch (IllegalArgumentException e1) {
            System.err.println(e1.getMessage());
            return;
        }

        searcher.runTopics();
        searcher.close();
        final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        LOG.info("Total run time: " + DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private List<TaggedSimilarity> constructSimilarities() {
        List<TaggedSimilarity> similarities = new ArrayList<>();

        for (String k1 : args.bm25_k1) {
            for (String b : args.bm25_b) {
                similarities.add(new TaggedSimilarity(new BM25Similarity(Float.parseFloat(k1), Float.parseFloat(b)),
                        String.format("bm25(k1=%s,b=%s)", k1, b)));
            }
        }

        return similarities;
    }

    private List<RerankerCascade> constructRerankers() {
        List<RerankerCascade> cascades = new ArrayList<>();

        RerankerCascade cascade = new RerankerCascade();
        cascade.add(new LambdaMARTReranker(Features.LAMBDAMART_DEFAULT_FEATURES));
        cascade.add(new ScoreTiesAdjusterReranker());
        cascades.add(cascade);

        return cascades;
    }

    public void runTopics() {
        TsvIntTopicReader tr;
        SortedMap<Integer, Map<String, String>> topics = new TreeMap<>();
        for (String singleTopicsFile : args.topics) {
            Path topicsFilePath = Paths.get(singleTopicsFile);
            if (!Files.exists(topicsFilePath) || !Files.isRegularFile(topicsFilePath) || !Files.isReadable(topicsFilePath)) {
                throw new IllegalArgumentException("Topics file : " + topicsFilePath + " does not exist or is not a (readable) file.");
            }
            try {
                tr = new TsvIntTopicReader(topicsFilePath);
                topics.putAll(tr.read());
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalArgumentException("Unable to load topic reader: " + args.topicReader);
            }
        }

        final String runTag = "bm25_ranklib";
        LOG.info("runtag: " + runTag);

        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(args.threads);
        List<TaggedSimilarity> similarities = constructSimilarities();
        List<RerankerCascade> cascades = constructRerankers();

        LOG.info("============ Launching Search Threads ============");

        for (TaggedSimilarity taggedSimilarity : similarities) {
            for (RerankerCascade cascade : cascades) {
                final String outputPath;

                if (similarities.size() == 1 && cascades.size() == 1) {
                    outputPath = args.output;
                } else {
                    outputPath = String.format("%s_%s_%s", args.output, taggedSimilarity.getTag(), cascade.getTag());
                }

                if (args.skipexists && new File(outputPath).exists()) {
                    LOG.info("Run already exists, skipping: " + outputPath);
                    continue;
                }
                executor.execute(new SearcherThread(reader, topics, taggedSimilarity, cascade, outputPath, runTag));
            }
        }
        executor.shutdown();

        try {
            // Wait for existing tasks to terminate
            while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    public ScoredDocuments search(IndexSearcher searcher, Integer qid, String queryString, RerankerCascade cascade) throws IOException {
        QueryGenerator generator = new BagOfWordsQueryGenerator();
        Query query = generator.buildQuery(IndexArgs.CONTENTS, analyzer, queryString);

        TopDocs rs = searcher.search(query, 1000);

        List<String> queryTokens = AnalyzerUtils.analyze(analyzer, queryString);

        RerankerContext<Integer> context = new RerankerContext<>(searcher, qid, query, null, queryString, queryTokens, null, args);
        ScoredDocuments scoredFbDocs = ScoredDocuments.fromTopDocs(rs, searcher);

        return cascade.run(scoredFbDocs, context);
    }

    private final class SearcherThread extends Thread {
        final private IndexSearcher searcher;
        final private SortedMap<Integer, Map<String, String>> topics;
        final private TaggedSimilarity taggedSimilarity;
        final private RerankerCascade cascade;
        final private String outputPath;
        final private String runTag;

        private SearcherThread(IndexReader reader, SortedMap<Integer, Map<String, String>> topics, TaggedSimilarity taggedSimilarity,
                               RerankerCascade cascade, String outputPath, String runTag) {
            this.topics = topics;
            this.taggedSimilarity = taggedSimilarity;
            this.cascade = cascade;
            this.runTag = runTag;
            this.outputPath = outputPath;
            this.searcher = new IndexSearcher(reader);
            this.searcher.setSimilarity(this.taggedSimilarity.getSimilarity());
            setName(outputPath);
        }

        @Override
        public void run() {
            try {
                String id = String.format("ranker: %s, reranker: %s", taggedSimilarity.getTag(), cascade.getTag());
                LOG.info("[Start] " + id);

                int cnt = 0;
                final long start = System.nanoTime();
                PrintWriter out = new PrintWriter(Files.newBufferedWriter(Paths.get(outputPath), StandardCharsets.US_ASCII));
                for (Map.Entry<Integer, Map<String, String>> entry : topics.entrySet()) {
                    Integer qid = entry.getKey();

                    String queryString = entry.getValue().get(args.topicfield);

                    ScoredDocuments docs = search(this.searcher, qid, queryString, cascade);

                    /*
                     * the first column is the topic number.
                     * the second column is currently unused and should always be "Q0".
                     * the third column is the official document identifier of the retrieved document.
                     * the fourth column is the rank the document is retrieved.
                     * the fifth column shows the score (integer or floating point) that generated the ranking.
                     * the sixth column is called the "run tag" and should be a unique identifier for your
                     */
                    int rank = 1;
                    for (int i = 0; i < docs.documents.length; i++) {
                        String docid = docs.documents[i].get(IndexArgs.ID);

                        out.println(String.format(Locale.US, "%s Q0 %s %d %f %s",
                                qid, docid, rank, docs.scores[i], runTag));

                        rank++;
                    }
                    cnt++;
                    if (cnt % 100 == 0) {
                        LOG.info(String.format("%d queries processed", cnt));
                    }
                }
                out.flush();
                out.close();
                final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);

                LOG.info("[End  ] " + id);
                LOG.info(topics.size() + " topics processed in " + DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));
            } catch (Exception e) {
                LOG.error(Thread.currentThread().getName() + ": Unexpected Exception:", e);
            }
        }
    }
}
