package org.n3r.flume.source.exec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ExecBlockSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ExecBlockSource.class);

    private String command;
    private CounterGroup counterGroup;
    private ExecutorService executor;
    private Future<?> runnerFuture;
    private long restartThrottle;
    private boolean restart;
    private boolean logStderr;
    private Integer bufferCount;
    private Integer bufferTimeout;
    private Pattern boundaryRegex;
    private ExecRunnable runner;

    @Override
    public void start() {
        logger.info("ExecEnhanced source starting with command:{}", command);

        executor = Executors.newSingleThreadExecutor();
        counterGroup = new CounterGroup();

        runner = new ExecRunnable(command, getChannelProcessor(), counterGroup,
                restart, restartThrottle, logStderr, bufferCount, bufferTimeout, boundaryRegex);

        // FIXME: Use a callback-like executor / future to signal us upon failure.
        runnerFuture = executor.submit(runner);

        /*
         * NB: This comes at the end rather than the beginning of the method because
         * it sets our state to running. We want to make sure the executor is alive
         * and well first.
         */
        super.start();

        logger.debug("ExecEnhanced source started");
    }

    @Override
    public void stop() {
        logger.info("Stopping execEnhanced source with command:{}", command);

        if (runner != null) {
            runner.setRestart(false);
            runner.kill();
        }
        if (runnerFuture != null) {
            logger.debug("Stopping execEnhanced runner");
            runnerFuture.cancel(true);
            logger.debug("ExecEnhanced runner stopped");
        }

        executor.shutdown();

        while (!executor.isTerminated()) {
            logger.debug("Waiting for execEnhanced executor service to stop");
            try {
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for execEnhanced executor service "
                        + "to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }

        super.stop();

        logger.debug("ExecEnhanced source with command:{} stopped. Metrics:{}", command,
                counterGroup);
    }

    @Override
    public void configure(Context context) {
        command = context.getString("command");

        Preconditions.checkState(command != null,
                "The parameter command must be specified");

        restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE,
                ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);

        restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART,
                ExecSourceConfigurationConstants.DEFAULT_RESTART);

        logStderr = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_LOG_STDERR, true);

        bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE,
                ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

        bufferTimeout = context.getInteger("batchTimeout", 3000);

        String regex = context.getString("boundaryRegex");
        boundaryRegex = StringUtils.isEmpty(regex) ? null : Pattern.compile(regex);

        logger.info("Configuration : restartThrottle=" + restartThrottle + ", restart=" + restart +
                ", logStderr=" + logStderr + ", bufferCount=" + bufferCount + ", bufferTimeout=" +
                bufferTimeout + ", boundaryRegex=" + regex);
    }

    private static class ExecRunnable implements Runnable {

        private static final String EXEC_ENHANCED_LINES_READ = "exec.enhanced.lines.read";

        public ExecRunnable(String command, ChannelProcessor channelProcessor,
                CounterGroup counterGroup, boolean restart, long restartThrottle,
                boolean logStderr, int bufferCount, int bufferTimeout, Pattern boundaryRegex) {
            this.command = command;
            this.channelProcessor = channelProcessor;
            this.counterGroup = counterGroup;
            this.restartThrottle = restartThrottle;
            this.bufferCount = bufferCount;
            this.bufferTimeout = bufferTimeout;
            this.restart = restart;
            this.logStderr = logStderr;
            this.boundaryRegex = boundaryRegex;
        }

        private String command;
        private ChannelProcessor channelProcessor;
        private CounterGroup counterGroup;
        private volatile boolean restart;
        private long restartThrottle;
        private int bufferCount;
        private int bufferTimeout;
        private boolean logStderr;
        private Pattern boundaryRegex;
        private Process process = null;
        private SystemClock systemClock = new SystemClock();
        private Long lastPushToChannel = systemClock.currentTimeMillis();
        private ScheduledExecutorService timedFlushService;
        private ScheduledFuture<?> future;
        private List<Event> eventList = new ArrayList<Event>();
        private StringBuilder block = new StringBuilder();

        @Override
        public void run() {
            do {
                String exitCode = "unknown";
                BufferedReader reader = null;
                try {
                    String[] commandArgs = command.split("\\s+");
                    process = new ProcessBuilder(commandArgs).start();
                    reader = new BufferedReader(
                            new InputStreamReader(process.getInputStream()));

                    // StderrLogger dies as soon as the input stream is invalid
                    StderrReader stderrReader = new StderrReader(new BufferedReader(
                            new InputStreamReader(process.getErrorStream())), logStderr);
                    stderrReader.setName("StderrReader-[" + command + "]");
                    stderrReader.setDaemon(true);
                    stderrReader.start();

                    String line = null;

                    startTimedFlushService();

                    while ((line = reader.readLine()) != null) {
                        synchronized (eventList) {
                            processLine(line);
                            if (eventList.size() >= bufferCount)
                                flushEventBatch(eventList);
                        }
                    }
                    synchronized (eventList) {
                        if (!eventList.isEmpty())
                            channelProcessor.processEventBatch(eventList);
                    }
                } catch (Exception e) {
                    logger.error("Failed while running command: " + command, e);
                    if (e instanceof InterruptedException)
                        Thread.currentThread().interrupt();
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException ex) {
                            logger.error("Failed to close reader for execEnhanced source", ex);
                        }
                    }
                    exitCode = String.valueOf(kill());
                }
                if (restart) {
                    logger.info("Restarting in {}ms, exit code {}", restartThrottle,
                            exitCode);
                    try {
                        Thread.sleep(restartThrottle);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    logger.info("Command [" + command + "] exited with " + exitCode);
                }
            } while (restart);
        }

        private void startTimedFlushService() {
            timedFlushService = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat(
                            "timedFlushExecService" + Thread.currentThread().getId()).build());

            future = timedFlushService.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            synchronized (eventList) {
                                if (!timeout())
                                    return;

                                if (StringUtils.isNotEmpty(block.toString()))
                                    flushBlock();

                                if(!eventList.isEmpty()) {
                                    logger.info("Timeout : eventList size is [{}]", eventList.size());
                                    flushEventBatch(eventList);
                                }
                            }
                        } catch (Exception e) {
                            logger.error("Exception occured when flushing event batch", e);
                            if(e instanceof InterruptedException)
                                Thread.currentThread().interrupt();
                        }
                    }
                }, bufferTimeout, bufferTimeout, TimeUnit.MILLISECONDS);
        }

        private void processLine(String line) {
            // Line break
            if (null == boundaryRegex) {
                logger.info("Line : {}", line);
                counterGroup.incrementAndGet(EXEC_ENHANCED_LINES_READ);
                eventList.add(EventBuilder.withBody(line.getBytes()));
                return;
            }

            // Block break
            Matcher matcher = boundaryRegex.matcher(line);
            int start = 0;
            while (matcher.find()) {
                // Incr counter
                counterGroup.incrementAndGet(EXEC_ENHANCED_LINES_READ);
                // Append the tail to last block before flushing it
                if (matcher.start() > 0) {
                    String tail = line.substring(start, matcher.start());
                    synchronized(block) {
                        block.append(tail);
                    }
                    // Reset the index of new block
                    start = matcher.start();
                }
                // Flush last block
                if (StringUtils.isNotEmpty(block.toString()))
                    flushBlock();
            }
            synchronized(block) {
                block.append(line.substring(start, line.length())).append("\n");
            }
        }

        private void flushBlock() {
            synchronized(block) {
                logger.info("Flush block [{}]", block.toString());
                eventList.add(EventBuilder.withBody(block.toString().getBytes()));
                block.delete(0, block.length());
            }
        }

        private void flushEventBatch(List<Event> eventList) {
            channelProcessor.processEventBatch(eventList);
            lastPushToChannel = systemClock.currentTimeMillis();
            eventList.clear();
        }

        private boolean timeout() {
            return (systemClock.currentTimeMillis() - lastPushToChannel) >= bufferTimeout;
        }

        public int kill() {
            if (process == null)
                return Integer.MIN_VALUE / 2;

            synchronized (process) {
                process.destroy();
                try {
                    int exitValue = process.waitFor();

                    // Stop the Thread that flushes periodically
                    if (future != null)
                        future.cancel(true);

                    if (timedFlushService != null) {
                        timedFlushService.shutdown();
                        while (!timedFlushService.isTerminated()) {
                            try {
                                timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                logger.debug("Interrupted while waiting for execEnhanced timed flush service "
                                  + "to stop. Just exiting.");
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                    return exitValue;
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            return Integer.MIN_VALUE;

        }

        public void setRestart(boolean restart) {
            this.restart = restart;
        }
    }

    private static class StderrReader extends Thread {
        private BufferedReader input;
        private boolean logStderr;

        protected StderrReader(BufferedReader input, boolean logStderr) {
            this.input = input;
            this.logStderr = logStderr;
        }

        @Override
        public void run() {
            try {
                int i = 0;
                String line = null;
                while ((line = input.readLine()) != null) {
                    if (logStderr)
                        logger.info("StderrLogger[{}] = '{}'", ++i, line);
                }
            } catch (IOException e) {
                logger.info("StderrLogger exiting", e);
            } finally {
                try {
                    if (input != null)
                        input.close();
                } catch (IOException ex) {
                    logger.error("Failed to close stderr reader for execEnhanced source", ex);
                }
            }
        }
    }

}
