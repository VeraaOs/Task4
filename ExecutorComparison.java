import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class ExecutorComparison {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    static class ThreadPerTaskExecutor implements ShutdownableExecutor {
        List<Thread> threads = new ArrayList<>();

        @Override
        public void execute(Runnable command) {
            var t = new Thread(command);
            threads.add(t);
            t.start();
        }

        public void shutdown() {
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {

        protected static final Runnable EXIT_TASK = () -> {
        };

        AtomicInteger counter = new AtomicInteger(0);

        List<Thread> threads;
        Vector<BlockingDeque<Runnable>> tasks;
        volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threadsCount) {
            Supplier<IntStream> stream = () -> IntStream.iterate(0, x -> x < threadsCount, x -> x + 1);
            tasks = new Vector<>(
                    stream.get()
                            .mapToObj(x -> new LinkedBlockingDeque<Runnable>())
                            .toList());
            this.threads = stream.get()
                    .mapToObj(this::spawnThread)
                    .toList();
            this.threads.forEach(Thread::start);
        }

        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task;
                    try {
                        task = tasks.get(id).takeFirst();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (task == EXIT_TASK)
                        return;
                    task.run();
                }
            });
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    tasks.forEach(queue -> queue.addLast(EXIT_TASK));
                }
            }
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown)
                throw new RejectedExecutionException("Executor is shutting down");
            tasks.get(counter.addAndGet(1) % threads.size()).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {

        WorkStealingExecutor(int threadsCount) {
            super(threadsCount);
        }

        @Override
        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task = tasks.get(id).pollFirst();
                    if (task == null)
                        task = stealTask(id);

                    if (task == null) {
                        try {
                            task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == null)
                            continue;
                    }

                    if (task == EXIT_TASK) {
                        task = stealTask(id);
                        if (task == null)
                            return;
                        tasks.get(id).addLast(EXIT_TASK);
                    }

                    task.run();
                }
            });
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                var queue = tasks.get((thiefId + offset) % tasks.size());
                var task = queue.pollLast();

                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }

                if (task != null)
                    return task;
            }
            return null;
        }
    }

    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 100_000;
    static final int TARGET_OPTIMAL_FULL_TIME = 10_000;

    static final int MEAN_TASK_TIME = (int) Math.round((TARGET_OPTIMAL_FULL_TIME + 0d) / TASK_NUMBER * THREAD_NUMBER);
    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2 + 1;
    static final int ESTIMATED_OPTIMAL_TIME = MEAN_TASK_TIME * TASK_NUMBER / THREAD_NUMBER;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = MEAN_TASK_TIME * 100;

    static final long BLACK_HOLE_ITERATIONS_PER_UNIT = 100_000L;

    private static void blackHole(long difficulty) {
        if (difficulty <= 0) {
            return;
        }
        long result = 0L;
        for (long i = 1; i <= difficulty; i++) {
            result += (i * 131542391L ^ (i << 3)) % 131542391L;
        }

        if (result == Long.MIN_VALUE) {
            System.nanoTime();
        }
    }

    static Runnable createTaskWithSleep(int duration) {
        return () -> {
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    static Runnable createTaskWithBlackHole(int duration) {
        return () -> blackHole((long) duration * BLACK_HOLE_ITERATIONS_PER_UNIT);
    }

    static int createTaskDuration(TaskDistribution distribution, int taskId, Random random) {
        return switch (distribution) {
            case UNIFORM -> random.nextInt(LOWER_TASK_TIME_BOUND, HIGHER_TASK_TIME_BOUND);
            case PERIODIC -> taskId % THREAD_NUMBER == 0
                    ? MEAN_TASK_TIME * THREAD_NUMBER
                    : 0;
            case PARETO -> {
                var scale = MEAN_TASK_TIME * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                var duration = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                yield (int) Math.min(Math.round(duration), PARETO_RAW_MAX_TASK_TIME);
            }
        };
    }

    static int[] createTaskDurations(TaskDistribution distribution) {
        var random = new Random(RANDOM_SEED);
        var durations = new int[TASK_NUMBER];

        long totalDuration = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            durations[taskId] = createTaskDuration(distribution, taskId, random);
            totalDuration += durations[taskId];
        }

        double scale = (double) TARGET_TOTAL_TASK_TIME / totalDuration;
        double remainder = 0;
        long normalizedTotal = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledDuration = durations[taskId] * scale + remainder;
            durations[taskId] = (int) scaledDuration;
            remainder = scaledDuration - durations[taskId];
            normalizedTotal += durations[taskId];
        }

        for (int taskId = 0; normalizedTotal < TARGET_TOTAL_TASK_TIME; taskId++) {
            durations[taskId % TASK_NUMBER]++;
            normalizedTotal++;
        }
        for (int taskId = 0; normalizedTotal > TARGET_TOTAL_TASK_TIME; taskId++) {
            int index = taskId % TASK_NUMBER;
            if (durations[index] > 0) {
                durations[index]--;
                normalizedTotal--;
            }
        }

        return durations;
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution, boolean useBlackHole) {
        var durations = createTaskDurations(distribution);
        return new Vector<>(
                IntStream
                        .iterate(0, x -> x < TASK_NUMBER, x -> x + 1)
                        .mapToObj(x -> useBlackHole
                                ? createTaskWithBlackHole(durations[x])
                                : createTaskWithSleep(durations[x]))
                        .toList()
        );
    }

    public record Pair<A, B>(A first, B second) {
    }

    public static Pair<Long, Long> measureExecutor(ShutdownableExecutor executor, TaskDistribution distribution, boolean useBlackHole) {
        var tasks = createTasks(distribution, useBlackHole);
        var start = System.nanoTime();
        tasks.forEach(executor::execute);
        var submissionEnd = System.nanoTime();
        executor.shutdown();
        var finish = System.nanoTime();
        return new Pair<>(submissionEnd - start, finish - start);
    }

    public static void printResults(String executorName, Pair<Long, Long> result) {
        if (result == null) {
            System.out.printf("%-40s замер пропущен%n", executorName);
            return;
        }
        System.out.printf("%-40s submission: %.3f ms, total: %.3f ms%n",
                executorName,
                result.first / 1_000_000d,
                result.second / 1_000_000d);
    }


    private static Pair<Long, Long> measureThreadPerTaskSafely(TaskDistribution distribution, boolean useBlackHole) {
        var executor = new ThreadPerTaskExecutor();
        try {
            return measureExecutor(executor, distribution, useBlackHole);
        } catch (Throwable e) {
            System.err.println("ThreadPerTaskExecutor: не удалось выполнить замер ("
                    + e + "), замер пропущен");
            return null;
        } finally {
            try {
                executor.shutdown();
            } catch (RuntimeException ignored) {
            }
        }
    }


    public static void main(String[] args) {
        System.out.println("=== Параметры эксперимента ===");
        System.out.println("THREAD_NUMBER = " + THREAD_NUMBER);
        System.out.println("TASK_NUMBER = " + TASK_NUMBER);
        System.out.println("TARGET_OPTIMAL_FULL_TIME = " + TARGET_OPTIMAL_FULL_TIME + " ms");
        System.out.println("ESTIMATED_OPTIMAL_TIME = " + ESTIMATED_OPTIMAL_TIME + " ms");
        System.out.println("MEAN_TASK_TIME = " + MEAN_TASK_TIME + " ms");
        System.out.println();

        System.out.println("Warm-up blackHole...");
        for (int i = 0; i < 3; i++) {
            blackHole(1_000_000);
        }
        System.out.println("Warm-up done.");
        System.out.println();

        System.out.println("ЧАСТЬ 1: Thread.sleep (имитация I/O-bound)");

        var threadPerTaskSleep = measureThreadPerTaskSafely(TaskDistribution.UNIFORM, false);
        printResults("ThreadPerTaskExecutor (sleep, UNIFORM)", threadPerTaskSleep);

        for (var distribution : TaskDistribution.values()) {
            System.out.println();
            System.out.println("--- Distribution: " + distribution + " ---");

            var roundRobinSleep = measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), distribution, false);
            printResults("RoundRobinExecutor (sleep)", roundRobinSleep);

            var workStealingSleep = measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), distribution, false);
            printResults("WorkStealingExecutor (sleep)", workStealingSleep);

            var fixedPoolSleep = measureExecutor(new FixedThreadPoolWrapper(THREAD_NUMBER), distribution, false);
            printResults("FixedThreadPoolWrapper (sleep)", fixedPoolSleep);
        }

        System.out.println();
        System.out.println("ЧАСТЬ 2: blackHole (CPU-bound)");

        var threadPerTaskCPU = measureThreadPerTaskSafely(TaskDistribution.UNIFORM, true);
        printResults("ThreadPerTaskExecutor (blackHole, UNIFORM)", threadPerTaskCPU);

        for (var distribution : TaskDistribution.values()) {
            System.out.println();
            System.out.println("--- Distribution: " + distribution + " ---");

            var roundRobinCPU = measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), distribution, true);
            printResults("RoundRobinExecutor (blackHole)", roundRobinCPU);

            var workStealingCPU = measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), distribution, true);
            printResults("WorkStealingExecutor (blackHole)", workStealingCPU);

            var fixedPoolCPU = measureExecutor(new FixedThreadPoolWrapper(THREAD_NUMBER), distribution, true);
            printResults("FixedThreadPoolWrapper (blackHole)", fixedPoolCPU);
        }

        System.out.println();
        System.out.println("Готово");
    }
}
