package redis.embedded;

import org.apache.commons.io.IOUtils;
import redis.embedded.exceptions.EmbeddedRedisException;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

abstract class AbstractRedisInstance implements Redis {
    protected List<String> args = Collections.emptyList();
    private volatile boolean active = false;
	private Process redisProcess;
    private final int port;

    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private PrintStream out = null; //Ignore Redis output.
    private PrintStream err = System.err; //Forward Redis error messages to STDERR.

    protected AbstractRedisInstance(int port) {
        this.port = port;
    }

    public boolean isActive() {
        return active;
    }

    public synchronized void start() throws EmbeddedRedisException {
        if (active) {
            throw new EmbeddedRedisException("This redis server instance is already running...");
        }
        try {
            redisProcess = createRedisProcessBuilder().start();
            logErrors();
            awaitRedisServerReady();
            active = true;
        } catch (IOException e) {
            throw new EmbeddedRedisException("Failed to start Redis instance", e);
        }
    }

    private void logErrors() {
        final InputStream errorStream = redisProcess.getErrorStream();
        copyStreamInBackground(errorStream, err);
    }

	private void copyStreamInBackground(final InputStream copyFrom, PrintStream copyTo) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(copyFrom));
        Runnable printReaderTask = new PrintReaderRunnable(reader, copyTo);
        executor.submit(printReaderTask);
	}

	public void outTo(PrintStream out) {
		this.out = out;
	}

	public void errTo(PrintStream err) {
		this.err = err;
	}
    
    private void awaitRedisServerReady() throws IOException {
    	InputStream stdoutStream = redisProcess.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stdoutStream));
        try {
            StringBuffer outputStringBuffer = new StringBuffer();
            String outputLine;
            do {
                outputLine = reader.readLine();
                if (outputLine == null) {
                    //Something goes wrong. Stream is ended before server was activated.
                    throw new RuntimeException("Can't start redis server. Check logs for details. Redis process log: "+outputStringBuffer.toString());
                }
                else{
                    outputStringBuffer.append("\n");
                    outputStringBuffer.append(outputLine);
                    if(out != null) {
                    	out.println(outputLine);
                    }
                }
            } while (!outputLine.matches(redisReadyPattern()));
        } finally {
        	if(out != null) {
        		/* Continue reading of STDOUT in a background thread. */
        		copyStreamInBackground(stdoutStream, out);
        	} else {
        		IOUtils.closeQuietly(reader);
        	}
        }
    }

    protected abstract String redisReadyPattern();

    private ProcessBuilder createRedisProcessBuilder() {
        File executable = new File(args.get(0));
        ProcessBuilder pb = new ProcessBuilder(args);
        pb.directory(executable.getParentFile());
        return pb;
    }

    public synchronized void stop() throws EmbeddedRedisException {
        if (active) {
            if (executor != null && !executor.isShutdown()) {
                executor.shutdown();
            }
            redisProcess.destroy();
            tryWaitFor();
            active = false;
        }
    }

    private void tryWaitFor() {
        try {
            redisProcess.waitFor();
        } catch (InterruptedException e) {
            throw new EmbeddedRedisException("Failed to stop redis instance", e);
        }
    }

    public List<Integer> ports() {
        return Arrays.asList(port);
    }

    private static class PrintReaderRunnable implements Runnable {
        private final BufferedReader reader;
		private final PrintStream outputStream;

        private PrintReaderRunnable(BufferedReader reader, PrintStream outputStream) {
            this.reader = reader;
            this.outputStream = outputStream;
        }

        public void run() {
            try {
                readLines();
            } finally {
                IOUtils.closeQuietly(reader);
            }
        }

        public void readLines() {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                	outputStream.println(line);
                }
            } catch (IOException e) {
                /* reader has been closed. The connected Redis instance has possibly shut down. */
            }
        }
    }
}
