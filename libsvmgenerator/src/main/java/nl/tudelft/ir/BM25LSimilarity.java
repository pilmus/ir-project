package nl.tudelft.ir;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;

import java.util.ArrayList;
import java.util.List;

class BM25LSimilarity extends Similarity {
    private final float k1;
    private final float b;
    private final float d;

    protected boolean discountOverlaps;
    private static final float[] LENGTH_TABLE = new float[256];


    public BM25LSimilarity(float k1, float b, float d) {
        this.discountOverlaps = true;
        if (Float.isFinite(k1) && !(k1 < 0.0F)) {
            if (!Float.isNaN(b) && !(b < 0.0F) && !(b > 1.0F)) {
                if (!Float.isNaN(d) && !(d < 0.0f) && !(d > 1.5f)) {

                    this.k1 = k1;
                    this.b = b;


                    this.d = d;
                } else {
                    throw new IllegalArgumentException("illegal d value: " + d + ", must be between 0 and 1.5");
                }
            } else {
                throw new IllegalArgumentException("illegal b value: " + b + ", must be between 0 and 1");
            }
        } else {
            throw new IllegalArgumentException("illegal k1 value: " + k1 + ", must be a non-negative finite value");
        }
    }

    public BM25LSimilarity() {
        this(1.8f, 0.3f, 0.5f);
    }

    protected float idf(long docFreq, long docCount) {
        return (float) Math.log(1.0D + ((double) (docCount - docFreq) + 0.5D) / ((double) docFreq + 0.5D));
    }

    /**
     * Copied from <code>BM25Similarity</code>
     */
    protected float scorePayload(int doc, int start, int end, BytesRef payload) {
        return 1.0F;
    }

    /**
     * Copied from <code>BM25Similarity</code>
     */
    protected float avgFieldLength(CollectionStatistics collectionStats) {
        return (float) ((double) collectionStats.sumTotalTermFreq() / (double) collectionStats.docCount());
    }

    /**
     * Copied from <code>BM25Similarity</code>
     */
    public void setDiscountOverlaps(boolean v) {
        this.discountOverlaps = v;
    }

    /**
     * Copied from <code>BM25Similarity</code>
     */
    public boolean getDiscountOverlaps() {
        return this.discountOverlaps;
    }

    /**
     * Compute the length of a document.
     * Copied from <code>BM25Similarity</code>
     *
     * @param state
     * @return
     */
    public final long computeNorm(FieldInvertState state) {
        int numTerms;

        numTerms = state.getLength();


        return SmallFloat.intToByte4(numTerms);
    }

    public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
        long df = termStats.docFreq();
        long docCount = collectionStats.docCount();
        float idf = this.idf(df, docCount);
        return Explanation.match(idf,
                "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:",
                Explanation.match(df, "n, number of documents containing term"),
                Explanation.match(docCount, "N, total number of documents with field"));
    }

    public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics[] termStats) {
        double idf = 0.0D;
        List<Explanation> details = new ArrayList();

        for (int i = 0; i < termStats.length; ++i) {
            TermStatistics stat = termStats[i];
            Explanation idfExplain = this.idfExplain(collectionStats, stat);
            details.add(idfExplain);
            idf += idfExplain.getValue().floatValue();
        }

        return Explanation.match((float) idf, "idf, sum of:", details);
    }

    public final SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        Explanation idf = termStats.length == 1 ? this.idfExplain(collectionStats, termStats[0]) : this.idfExplain(collectionStats, termStats);
        float avgdl = this.avgFieldLength(collectionStats);
        float[] cache = new float[256];

        for (int i = 0; i < cache.length; ++i) {
            cache[i] = this.k1 * (1.0F - this.b + this.b * LENGTH_TABLE[i] / avgdl);
        }

        return new BM25LScorer(boost, this.k1, this.b, this.d, idf, avgdl, cache);
    }

    public String toString() {
        return "BM25(k1=" + this.k1 + ",b=" + this.b + ")";
    }

    public final float getK1() {
        return this.k1;
    }

    public final float getB() {
        return this.b;
    }

    static {
        for (int i = 0; i < 256; ++i) {
            LENGTH_TABLE[i] = (float) SmallFloat.byte4ToInt((byte) i);
        }

    }

    private static class BM25LScorer extends SimScorer {
        private final float boost;
        private final float k1;
        private final float b;
        private final float d;
        private final Explanation idf;
        private final float avgdl;
        private final float[] cache;
        private final float weight;

        BM25LScorer(float boost, float k1, float b, float d, Explanation idf, float avgdl, float[] cache) {
            this.boost = boost;
            this.idf = idf;
            this.avgdl = avgdl;
            this.k1 = k1;
            this.b = b;
            this.d = d;
            this.cache = cache;
            this.weight = (k1 + 1) * boost * idf.getValue().floatValue();
        }

        /**
         * Score is computed as follows:
         *
         * score = idf * (k1 + 1) (ctd + d) / (k1 + ctd + d)
         *
         * ctd = freq / (1 - b + b * (doclen/avglen))
         * norm = k1 * (1 - b + b * (doclen/avglen)) --> (1 - b + b * (doclen/avglen)) = norm / k1
         *
         * ctd = freq / (norm / k1) = k1 * freq / norm
         *
         * score = idf * (k1 + 1) (k1 * freq / norm + d) / (k1 + k1 * freq / norm + d)
         *
         * @param freq
         * @param encodedNorm
         * @return
         */
        public float score(float freq, long encodedNorm) {
            double norm = this.cache[(byte) ((int) encodedNorm) & 255];
//            idf * (k1 + 1) (k1 * freq / norm + d) / (k1 + k1 * freq / norm + d)
            return (float) (this.boost * this.idf.getValue().floatValue() * (this.k1 + 1) * (this.k1 * freq / norm + this.d) / (this.k1 + this.k1 * freq / norm + d));

        }

        public Explanation explain(Explanation freq, long encodedNorm) {
            List<Explanation> subs = new ArrayList(this.explainConstantFactors());
            Explanation tfExpl = this.explainTFNorm(freq, encodedNorm);
            subs.add(tfExpl);
            return Explanation.match(this.weight * tfExpl.getValue().floatValue(), "score(freq=" + freq.getValue() + "), product of:", subs);
        }

        private Explanation explainTFNorm(Explanation freq, long norm) {
            List<Explanation> subs = new ArrayList();
            subs.add(freq);
            subs.add(Explanation.match(this.k1, "k1, term saturation parameter", new Explanation[0]));
            float doclen = BM25LSimilarity.LENGTH_TABLE[(byte) ((int) norm) & 255];
            subs.add(Explanation.match(this.b, "b, length normalization parameter", new Explanation[0]));
            subs.add(Explanation.match(this.d, "d, long document penalization parameter", new Explanation[0]));
            if ((norm & 255L) > 39L) {
                subs.add(Explanation.match(doclen, "dl, length of field (approximate)", new Explanation[0]));
            } else {
                subs.add(Explanation.match(doclen, "dl, length of field", new Explanation[0]));
            }

            subs.add(Explanation.match(this.avgdl, "avgdl, average length of field", new Explanation[0]));
            float tfNormValue = d + (float) freq.getValue() / (1 - b + b * doclen / this.avgdl);
            subs.add(Explanation.match(tfNormValue, "tfNormValue, computed as d + freq / (1 - b + b * dl / avgdl)"));
            return Explanation.match(((k1 + 1) * tfNormValue) / (k1 + tfNormValue),
                    "tfNorm, computed as ((k1 + 1) * tfNormValue) / (k1 + tfNormValue) from:", subs);
        }

        private List<Explanation> explainConstantFactors() {
            List<Explanation> subs = new ArrayList();
            if (this.boost != 1.0F) {
                subs.add(Explanation.match(this.boost, "boost", new Explanation[0]));
            }

            subs.add(this.idf);
            return subs;
        }
    }
}
