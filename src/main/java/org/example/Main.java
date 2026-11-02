package org.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз;
 * - для каждого URL requests = successful + errors;
 * - maxDuration <= totalDuration при наличии запросов;
 * - 0 <= activeFiles <= semaphoreLimit;
 * - после завершения анализа activeFiles равно нулю;
 * - один повреждённый файл не мешает обработке остальных;
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */
public class Main {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}

    /**
     * Возвращает отдельный итератор для чтения строк одного файла.
     *
     * ВАЖНО: Iterator<String> потоконебезопасен. Один экземпляр нельзя
     * одновременно читать из нескольких потоков. Для каждого потока нужен
     * отдельный вызов LogFileLines.iterator(path).
     */
    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    private void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    /** Генерирует реальные текстовые файлы с детерминированными данными. */
    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }

    /** Проверяет генератор и чтение строк без участия студенческого анализатора. */
    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Unexpected line: " + line
                        );
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }

    public record AnalysisResult(
            Report report,
            Map<String, PathStatistics> byPath,
            List<String> failedFiles,
            long processedFiles,
            long readLines,
            long validEntries,
            long malformedEntries,
            long readErrors,
            int activeFiles,
            int maxActiveFiles,
            boolean stopped
    ) {}

    public static LogEntry parse(String line) {
        String[] fields = line.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException("Ожидалось 5 полей: " + line);
        }
        try {
            int status = Integer.parseInt(fields[2]);
            long bytes = Long.parseLong(fields[3]);
            long duration = Long.parseLong(fields[4]);
            if (status < 200 || status > 599 || bytes < 0 || duration < 0) {
                throw new IllegalArgumentException("Некорректные значения: " + line);
            }
            return new LogEntry(fields[0], fields[1], status, bytes, duration);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Некорректное число: " + line);
        }
    }

    static class PathAccumulator {
        long requests;
        long successful;
        long errors;
        long responseBytes;
        long totalDuration;
        long maxDuration;

        void add(LogEntry entry) {
            requests++;
            if (entry.status() <= 399) {
                successful++;
            } else {
                errors++;
            }
            responseBytes += entry.responseBytes();
            totalDuration += entry.durationMillis();
            maxDuration = Math.max(maxDuration, entry.durationMillis());
        }

        PathStatistics toStatistics() {
            return new PathStatistics(requests, successful, errors,
                    responseBytes, totalDuration, maxDuration);
        }
    }

    interface StatisticsAccumulator {
        void add(LogEntry entry);

        Map<String, PathStatistics> snapshot();
    }

    static class GlobalLockAccumulator implements StatisticsAccumulator {
        private final Map<String, PathAccumulator> byPath = new HashMap<>();

        public synchronized void add(LogEntry entry) {
            byPath.computeIfAbsent(entry.path(), p -> new PathAccumulator()).add(entry);
        }

        public synchronized Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> result = new TreeMap<>();
            byPath.forEach((path, acc) -> result.put(path, acc.toStatistics()));
            return result;
        }
    }

    static class PerPathAccumulator implements StatisticsAccumulator {
        private final ConcurrentHashMap<String, PathAccumulator> byPath = new ConcurrentHashMap<>();

        public void add(LogEntry entry) {
            PathAccumulator acc = byPath.computeIfAbsent(entry.path(), p -> new PathAccumulator());
            synchronized (acc) {
                acc.add(entry);
            }
        }

        public Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> result = new TreeMap<>();
            byPath.forEach((path, acc) -> {
                synchronized (acc) {
                    result.put(path, acc.toStatistics());
                }
            });
            return result;
        }
    }

    static Report buildReport(Map<String, PathStatistics> byPath) {
        long total = 0, successful = 0, errors = 0, bytes = 0, max = 0;
        for (PathStatistics s : byPath.values()) {
            total += s.requests();
            successful += s.successful();
            errors += s.errors();
            bytes += s.responseBytes();
            max = Math.max(max, s.maxDurationMillis());
        }
        return new Report(total, successful, errors, bytes, max);
    }

    static AnalysisResult analyzeSequential(List<LogFile> files) {
        GlobalLockAccumulator stats = new GlobalLockAccumulator();
        List<String> failed = new ArrayList<>();
        long processed = 0, lines = 0, valid = 0, malformed = 0;

        for (LogFile file : files) {
            try {
                Iterator<String> it = LogFileLines.iterator(file.path());
                while (it.hasNext()) {
                    String line = it.next();
                    lines++;
                    try {
                        stats.add(parse(line));
                        valid++;
                    } catch (IllegalArgumentException e) {
                        malformed++;
                    }
                }
                processed++;
            } catch (RuntimeException e) {
                failed.add(file.path().getFileName() + ": " + e.getMessage());
            }
        }

        Map<String, PathStatistics> byPath = stats.snapshot();
        return new AnalysisResult(buildReport(byPath), byPath, failed, processed, lines,
                valid, malformed, failed.size(), 0, 1, false);
    }

    static class ParallelAnalyzer {
        private final int threads;
        private final int semaphoreLimit;
        private final boolean perPathLocks;
        private final Semaphore semaphore;
        private volatile boolean stopped;

        private final AtomicInteger processedFiles = new AtomicInteger();
        private final AtomicLong readLines = new AtomicLong();
        private final AtomicLong validEntries = new AtomicLong();
        private final AtomicLong malformedEntries = new AtomicLong();
        private final AtomicInteger readErrors = new AtomicInteger();
        private final AtomicInteger activeFiles = new AtomicInteger();
        private final AtomicInteger maxActiveFiles = new AtomicInteger();
        private final List<String> failedFiles = Collections.synchronizedList(new ArrayList<>());

        ParallelAnalyzer(int threads, int semaphoreLimit, boolean perPathLocks) {
            this.threads = threads;
            this.semaphoreLimit = semaphoreLimit;
            this.perPathLocks = perPathLocks;
            this.semaphore = new Semaphore(semaphoreLimit);
        }

        void stop() {
            stopped = true;
        }

        AnalysisResult analyze(List<LogFile> files) throws InterruptedException {
            StatisticsAccumulator stats = perPathLocks
                    ? new PerPathAccumulator()
                    : new GlobalLockAccumulator();
            CountDownLatch latch = new CountDownLatch(files.size());
            ExecutorService executor = Executors.newFixedThreadPool(threads);

            for (LogFile file : files) {
                executor.submit(() -> {
                    try {
                        processFile(file, stats);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);

            Map<String, PathStatistics> byPath = stats.snapshot();
            return new AnalysisResult(buildReport(byPath), byPath, new ArrayList<>(failedFiles),
                    processedFiles.get(), readLines.get(), validEntries.get(),
                    malformedEntries.get(), readErrors.get(),
                    activeFiles.get(), maxActiveFiles.get(), stopped);
        }

        private void processFile(LogFile file, StatisticsAccumulator stats) {
            if (stopped) {
                return;
            }
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                int active = activeFiles.incrementAndGet();
                updateMax(active);
                readFile(file, stats);
            } catch (RuntimeException e) {
                readErrors.incrementAndGet();
                failedFiles.add(file.path().getFileName() + ": " + e.getMessage());
            } finally {
                activeFiles.decrementAndGet();
                semaphore.release();
            }
        }

        private void readFile(LogFile file, StatisticsAccumulator stats) {
            Iterator<String> it = LogFileLines.iterator(file.path());
            while (it.hasNext()) {
                if (stopped) {
                    return;
                }
                String line = it.next();
                readLines.incrementAndGet();
                try {
                    stats.add(parse(line));
                    validEntries.incrementAndGet();
                } catch (IllegalArgumentException e) {
                    malformedEntries.incrementAndGet();
                }
            }
            processedFiles.incrementAndGet();
        }

        private void updateMax(int active) {
            int current = maxActiveFiles.get();
            while (active > current && !maxActiveFiles.compareAndSet(current, active)) {
                current = maxActiveFiles.get();
            }
        }
    }

    static List<LogFile> listLogFiles(Path directory) throws IOException {
        try (var paths = Files.list(directory)) {
            return paths.sorted().map(LogFile::new).toList();
        }
    }

    static void printResult(String title, AnalysisResult r) {
        System.out.println("=== " + title + " ===");
        Report rep = r.report();
        System.out.println("Запросов: " + rep.totalRequests()
                + ", успешных: " + rep.successfulRequests()
                + ", ошибок: " + rep.errorRequests()
                + ", байт: " + rep.responseBytes()
                + ", макс. время: " + rep.maxDurationMillis() + " мс");
        r.byPath().forEach((path, s) -> System.out.println("  " + path
                + ": запросов " + s.requests()
                + ", успешных " + s.successful()
                + ", ошибок " + s.errors()
                + ", байт " + s.responseBytes()
                + ", время " + s.totalDurationMillis()
                + ", макс. " + s.maxDurationMillis()));
        System.out.println("Обработано файлов: " + r.processedFiles()
                + ", строк: " + r.readLines()
                + ", корректных: " + r.validEntries()
                + ", повреждённых: " + r.malformedEntries()
                + ", ошибок чтения: " + r.readErrors());
        System.out.println("Активных файлов после: " + r.activeFiles()
                + ", максимум активных: " + r.maxActiveFiles()
                + ", остановлен: " + (r.stopped() ? "да" : "нет"));
        System.out.println("Необработанные файлы: " + r.failedFiles());
        System.out.println();
    }

    static void generateCustom(Path directory, int fileCount, int lines, boolean oneUrl)
            throws IOException {
        Files.createDirectories(directory);
        Random random = new Random(1);
        for (int i = 0; i < fileCount; i++) {
            Path file = directory.resolve("access-%03d.log".formatted(i));
            try (BufferedWriter writer = Files.newBufferedWriter(file)) {
                for (int j = 0; j < lines; j++) {
                    String path = oneUrl ? "/api/users" : "/item/" + random.nextInt(10_000);
                    writer.write("GET " + path + " 200 " + random.nextInt(10_000)
                            + " " + (1 + random.nextInt(500)) + "\n");
                }
            }
        }
    }

    static long measure(List<LogFile> files, int threads, int limit, boolean perPath)
            throws InterruptedException {
        long start = System.currentTimeMillis();
        new ParallelAnalyzer(threads, limit, perPath).analyze(files);
        return System.currentTimeMillis() - start;
    }

    static void runAll() throws Exception {
        Path logs = Path.of("logs");
        GenerateLogs.generate(logs, 20, 1_000);
        List<LogFile> files = listLogFiles(logs);

        AnalysisResult sequential = analyzeSequential(files);
        printResult("Последовательный отчёт", sequential);

        ParallelAnalyzer analyzer = new ParallelAnalyzer(4, 2, true);
        AnalysisResult parallel = analyzer.analyze(files);
        printResult("Параллельный отчёт (4 потока, семафор 2)", parallel);
        System.out.println("Отчёты совпадают: " + (sequential.byPath().equals(parallel.byPath()) ? "да" : "нет"));
        System.out.println();

        Path big = Path.of("logs-big");
        GenerateLogs.generate(big, 20, 20_000);
        List<LogFile> bigFiles = listLogFiles(big);

        System.out.println("1 поток: " + measure(bigFiles, 1, 4, true) + " мс");
        System.out.println("4 потока: " + measure(bigFiles, 4, 4, true) + " мс");
        System.out.println("Семафор 1: " + measure(bigFiles, 4, 1, true) + " мс");
        System.out.println("Семафор 4: " + measure(bigFiles, 4, 4, true) + " мс");

        Path oneUrl = Path.of("logs-one-url");
        generateCustom(oneUrl, 20, 20_000, true);
        Path manyUrls = Path.of("logs-many-urls");
        generateCustom(manyUrls, 20, 20_000, false);
        List<LogFile> oneUrlFiles = listLogFiles(oneUrl);
        List<LogFile> manyUrlFiles = listLogFiles(manyUrls);

        System.out.println("Один URL, глобальная блокировка: " + measure(oneUrlFiles, 4, 4, false) + " мс");
        System.out.println("Один URL, блокировки по URL: " + measure(oneUrlFiles, 4, 4, true) + " мс");
        System.out.println("Много URL, глобальная блокировка: " + measure(manyUrlFiles, 4, 4, false) + " мс");
        System.out.println("Много URL, блокировки по URL: " + measure(manyUrlFiles, 4, 4, true) + " мс");
        System.out.println();

        Path broken = Path.of("logs-broken");
        GenerateLogs.generate(broken, 5, 1_000);
        Path first = broken.resolve("access-000.log");
        Files.writeString(first, Files.readString(first) + "GET /bad 200 abc 5\n");
        Files.write(broken.resolve("access-999.log"), new byte[]{(byte) 0xFF, (byte) 0xFE});
        List<LogFile> brokenFiles = new ArrayList<>(listLogFiles(broken));
        brokenFiles.add(new LogFile(broken.resolve("missing.log")));
        printResult("Повреждённые данные", new ParallelAnalyzer(4, 2, true).analyze(brokenFiles));

        ParallelAnalyzer stoppable = new ParallelAnalyzer(4, 2, true);
        Thread stopper = new Thread(() -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                return;
            }
            stoppable.stop();
        });
        stopper.start();
        AnalysisResult partial = stoppable.analyze(bigFiles);
        printResult("Досрочная остановка", partial);
        System.out.println("Свободных разрешений семафора: " + stoppable.semaphore.availablePermits()
                + " из " + stoppable.semaphoreLimit);
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;
            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );
            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        if (args.length == 0) {
            runAll();
            return;
        }

        System.out.println(
                "Usage: java Main "
                        + "generate <directory> <files> <lines>"
        );
        System.out.println(
                "       java Main check"
        );
    }
}