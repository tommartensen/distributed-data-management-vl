package de.hpi.ddm;

// inspired by TaxiRideSource from Flink Exercises

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class LogEntrySource implements SourceFunction<LogEntry> {

    private final String dataFilePath;

    private transient BufferedReader reader;
    private transient FileInputStream fileStream;

    public LogEntrySource(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    @Override
    public void run(SourceContext<LogEntry> sourceContext) throws Exception {

        fileStream = new FileInputStream(dataFilePath);
        reader = new BufferedReader(new InputStreamReader(fileStream, StandardCharsets.UTF_8));

        generateStream(sourceContext);

        this.reader.close();
        this.reader = null;
        this.fileStream.close();
        this.fileStream = null;
    }

    private void generateStream(SourceContext<LogEntry> sourceContext) throws IOException {
        String line;

        while (reader.ready() && (line = reader.readLine()) != null) {
            LogEntry logEntry = LogEntry.fromString(line);
            if (logEntry != null) {
                sourceContext.collect(logEntry);
            }
        }
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.fileStream != null) {
                this.fileStream.close();
            }

        } catch(IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.fileStream = null;
        }
    }

}

