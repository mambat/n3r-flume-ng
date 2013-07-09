package org.n3r.flume.inspect.exec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 测试ExecSource
 * 1）events发送至channel只有batchSize控制，而没有batchTimeout
 * 2）In case of replicating flow, and a sink is allowed to bind one and only one channel:
 *    适当提高batchSize的值，可以实现exception stack grabed as a whole
 * 3）needing batchTimeout control
 *
 * @author wanglei
 *
 */
public class ExecSource {

    public static void main(String[] args) {
        String command = "tail -F /home/wanglei/time.log";
        System.out.println("Exec source starting with command >> " + command);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ExecRunnable runner = new ExecRunnable(command, 20);
        Future<?> runnerFuture = executor.submit(runner);
        System.out.println("Exec source started");
    }

    private static class ExecRunnable implements Runnable {

        public ExecRunnable(String command, int bufferCount) {
            this.command = command;
            this.bufferCount = bufferCount;
        }

        private String command;
        private int bufferCount;
        private Process process = null;

        @Override
        public void run() {
            String exitCode = "unknown";
            BufferedReader reader = null;
            try {
                String[] commandArgs = command.split("\\s+");
                process = new ProcessBuilder(commandArgs).start();
                reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream()));

                int count = 0;
                String line = null;
                List<Integer> eventList = new ArrayList<Integer>();
                while ((line = reader.readLine()) != null) {
                    System.out.println("readline >> " + line + " >> " + new Date());
                    eventList.add(++count);
                    if (eventList.size() >= bufferCount) {
                        System.out.println("reach bufferCount >> " + bufferCount + " >> " + eventList.size());
                        eventList.clear();
                    }
                }
                if (!eventList.isEmpty()) {
                    System.out.println("has jumped out the while loop, eventList size >> " + eventList.size());
                }
            } catch (Exception e) {
                System.out.println("Failed while running command: " + command);
                e.printStackTrace();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException ex) {
                        System.out.println("Failed to close reader for exec source");
                        ex.printStackTrace();
                    }
                }
                exitCode = String.valueOf(kill());
            }
            System.out.println("Command [" + command + "] exited with >> " + exitCode);
        }

        public int kill() {
            if (process != null) {
                synchronized (process) {
                    process.destroy();
                    try {
                        return process.waitFor();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Integer.MIN_VALUE;
            }
            return Integer.MIN_VALUE / 2;
        }
    }
}
