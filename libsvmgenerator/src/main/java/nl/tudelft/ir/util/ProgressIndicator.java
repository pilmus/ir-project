package nl.tudelft.ir.util;

import java.util.concurrent.atomic.AtomicInteger;

public class ProgressIndicator implements Runnable {
    private final static long UPDATE_INTERVAL = 500;

    private final AtomicInteger counter;
    private final long total;

    public ProgressIndicator(AtomicInteger counter, long total) {
        this.counter = counter;
        this.total = total;
    }

    @Override
    public void run() {
        int lastCount = 0;
        try {
            while (lastCount < total) {
                int count = counter.get();
                int difference = count - lastCount;
                double iterationsPerSec = difference * (1000.0 / (double) UPDATE_INTERVAL);
                String percentage = String.format("%.2f", ((double) count / total) * 100);
                System.out.println(percentage + "% " + count + "/" + total + " [" + String.format("%.2f", iterationsPerSec) + " it/s]");

                lastCount = count;

                Thread.sleep(UPDATE_INTERVAL);
            }
        } catch (InterruptedException ignored) {
        }
    }
}
