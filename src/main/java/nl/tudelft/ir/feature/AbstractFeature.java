package nl.tudelft.ir.feature;

import io.anserini.analysis.AnalyzerUtils;
import io.anserini.index.IndexArgs;
import io.anserini.index.IndexReaderUtils;
import io.anserini.index.NotStoredException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractFeature implements Feature {
    protected double sum(double[] vector) {
        float s = 0;
        for (double e : vector) {
            s += e;
        }

        return s;
    }

    protected double[] multiply(double[] a, double[] b) {
        // element-wise multiply
        double[] result = new double[a.length];

        for (int i = 0; i < a.length; i++) {
            result[i] = a[i] * b[i];
        }

        return result;
    }
}
