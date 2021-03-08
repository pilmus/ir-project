package nl.tudelft.ir.util;

import java.util.concurrent.atomic.AtomicInteger;

public class ProgressIndicator implements Runnable {
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
