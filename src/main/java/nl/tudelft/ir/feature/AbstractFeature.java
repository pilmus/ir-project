package nl.tudelft.ir.feature;

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
