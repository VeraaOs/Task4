import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

interface Shutdownable {
    void shutdown();
}

interface ShutdownableExecutor extends Shutdownable, Executor {
}

public class FixedThreadPoolWrapper implements ShutdownableExecutor {

    private final ThreadPoolExecutor executor;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public FixedThreadPoolWrapper(int nThreads) {
        if (nThreads <= 0) {
            throw new IllegalArgumentException("nThreads must be > 0");
        }
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(nThreads);
    }

    @Override
    public void execute(Runnable command) {
        if (shuttingDown.get()) {
            throw new RejectedExecutionException("Executor is shutting down");
        }
        executor.execute(command);
    }

    @Override
    public void shutdown() {
        if (shuttingDown.compareAndSet(false, true)) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                    executor.awaitTermination(1, TimeUnit.MINUTES);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
                throw new RuntimeException(e);
            }
        }
    }

    public int getPoolSize() {
        return executor.getPoolSize();
    }

    public long getCompletedTaskCount() {
        return executor.getCompletedTaskCount();
    }

    public int getQueueSize() {
        return executor.getQueue().size();
    }
}
