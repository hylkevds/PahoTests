package paho.test.pahotests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class MultiPublishTest {

    public static final Charset UTF8 = Charset.forName("UTF8");
    public static final byte[] PAYLOAD = "test".getBytes(UTF8);

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiPublishTest.class);

    private static final String SERVER_URI = "tcp://localhost:1884";
    private static final String CLIENT_ID = "Tester-" + UUID.randomUUID();
    private static final int QOS = 0;

    private static final int WORKER_COUNT = 10;
    private static final long WORKER_DELAY = 1;
    /**
     * If publish takes longer than this, assume the worker hangs.
     */
    private static final long MAX_WORK_TIME = 1000;
    private static final long WATCHER_DELAY = 1000;
    private String topic = "testTopic";
    private MqttClient client;

    private List<Worker> workers = new ArrayList<>();
    private ScheduledExecutorService checkerExecutor;
    private ScheduledFuture<?> checker;

    public MultiPublishTest() throws MqttException {
        client = new MqttClient(SERVER_URI, CLIENT_ID);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setMaxInflight(1000);
        client.connect(options);
    }

    public void createWorkers() {
        LOGGER.info("Creating {} workers.", WORKER_COUNT);
        for (int i = 0; i < WORKER_COUNT; i++) {
            Worker worker = new Worker(client, topic, "Worker-" + i, WORKER_DELAY);
            Thread t = new Thread(worker);
            worker.setThread(t);
            workers.add(worker);
        }
        LOGGER.info("Creating worker-Watcher.");
        checkerExecutor = Executors.newSingleThreadScheduledExecutor();
        checker = checkerExecutor.scheduleWithFixedDelay(this::checkForHangs, WATCHER_DELAY, WATCHER_DELAY, TimeUnit.MILLISECONDS);
    }

    public void checkForHangs() {
        LOGGER.trace("Checking workers...");
        long now = System.currentTimeMillis();
        long cutOff = now - MAX_WORK_TIME;
        int hangCount = 0;
        for (Worker worker : workers) {
            long lastStart = worker.getLastStart();
            long lastEnd = worker.getLastEnd();
            if (lastEnd >= lastStart) {
                // Definitely not hanging.
                if (worker.isHanging()) {
                    worker.setHanging(false);
                }
                continue;
            }
            if (lastStart < cutOff) {
                if (!worker.isHanging()) {
                    LOGGER.warn("Worker {} seems to hang.", worker.getName());
                    worker.setHanging(true);
                }
            } else {
                if (worker.isHanging()) {
                    LOGGER.warn("Worker {} seems to have continued.", worker.getName());
                    worker.setHanging(false);
                }
            }
            if (worker.isHanging()) {
                hangCount++;
            }
        }
        LOGGER.info("Hanging threads: {}.", hangCount);
    }

    public void startWorkers() {
        for (Worker worker : workers) {
            LOGGER.info("Starting worker {}", worker.getName());
            worker.getThread().start();
        }
    }

    public void stopWorkers() {
        checker.cancel(true);
        checkerExecutor.shutdown();
        for (Worker worker : workers) {
            LOGGER.info("Stopping worker {}", worker.getName());
            worker.stop();
        }
        workers.clear();
        checkerExecutor.shutdownNow();
        checker = null;
        checkerExecutor = null;
    }

    public void close() {
        try {
            client.disconnect();
        } catch (MqttException ex) {
            LOGGER.error("Exception closing client", ex);
        }
        try {
            client.close();
        } catch (MqttException ex) {
            LOGGER.error("Exception closing client", ex);
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws UnsupportedEncodingException, MqttException, IOException {
        MultiPublishTest test = new MultiPublishTest();
        test.createWorkers();
        test.startWorkers();
        try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in, "UTF-8"))) {
            LOGGER.warn("Press Enter to exit.");
            input.read();
            LOGGER.warn("Exiting...");
        }
        test.stopWorkers();
        test.close();
        LOGGER.warn("Done...");
    }

    private static class Worker implements Runnable {

        /**
         * The logger for this class.
         */
        private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Worker.class);
        private final MqttClient client;
        private final String topic;
        private final String name;
        private final long delay;
        private long lastStart = 0;
        private long lastEnd = 0;
        private Thread thread;
        private boolean running = true;
        private boolean hanging = false;

        public Worker(MqttClient client, String topic, String name, long delay) {
            this.client = client;
            this.topic = topic;
            this.name = name;
            this.delay = delay;
        }

        public synchronized void stop() {
            running = false;
            this.notifyAll();
        }

        private synchronized void sleep() {
            try {
                this.wait(delay);
            } catch (InterruptedException ex) {
                LOGGER.debug("Rude wakeup.", ex);
            }
        }

        @Override
        public void run() {
            while (running) {
                try {
                    lastStart = System.currentTimeMillis();
                    client.publish(topic, PAYLOAD, QOS, false);
                } catch (MqttException ex) {
                    LOGGER.error("{} Failed to publish.", name);
                    LOGGER.debug("Failed to publish.", ex);
                }
                lastEnd = System.currentTimeMillis();
                sleep();
            }
        }

        public Thread getThread() {
            return thread;
        }

        public void setThread(Thread thread) {
            this.thread = thread;
        }

        public long getLastStart() {
            return lastStart;
        }

        public long getLastEnd() {
            return lastEnd;
        }

        public void setHanging(boolean hanging) {
            this.hanging = hanging;
        }

        public boolean isHanging() {
            return hanging;
        }

        public String getName() {
            return name;
        }

    }
}
