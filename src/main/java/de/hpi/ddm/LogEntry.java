package de.hpi.ddm;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.log4j.Logger;

import java.util.Locale;

public class LogEntry {
    private final static Logger logger = Logger.getLogger(LogEntry.class);

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public LogEntry() {
        this.timestamp = new DateTime();
    }

    public LogEntry(String accessor, DateTime timestamp, String httpMethod, String resource, String httpVersion, int httpStatus, int bytesTransferred) {
        this.accessor = accessor;
        this.timestamp = timestamp;
        this.httpMethod = httpMethod;
        this.resource = resource;
        this.httpVersion = httpVersion;
        this.httpStatus = httpStatus;
        this.bytesTransferred = bytesTransferred;
    }

    public String accessor;
    public DateTime timestamp;
    public String httpMethod;
    public String resource;
    public String httpVersion;
    public int httpStatus;
    public int bytesTransferred;


    // TODO: Starting point for parsing the log entry from the log
    // Examples:
    // in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839
    // 133.43.96.45 - - [01/Aug/1995:00:00:16 -0400] "GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0" 200 10566
    public static LogEntry fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 7) {
            logger.warn("Invalid record: " + line);
            return new LogEntry();
        }

        LogEntry logEntry = new LogEntry();

        try {
            logEntry.timestamp = DateTime.parse(tokens[2], timeFormatter);
            logEntry.accessor = tokens[8];
            logEntry.httpMethod = tokens[5];
            logEntry.resource = tokens[6];
            logEntry.httpVersion = tokens[8];
            logEntry.httpStatus = Integer.parseInt(tokens[8]);
            logEntry.bytesTransferred = Integer.parseInt(tokens[9]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return logEntry;
    }

    // sort by timestamp,
    public int compareTo(LogEntry other) {
        if (other == null) {
            return 1;
        }
        return Long.compare(this.getEventTime(), other.getEventTime());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LogEntry &&
                this.timestamp == ((LogEntry) other).timestamp
                && this.accessor.equals(((LogEntry) other).accessor)
                && this.bytesTransferred == ((LogEntry) other).bytesTransferred;
    }

    @Override
    public int hashCode() {
        return (int) this.timestamp.getMillis();
    }

    public long getEventTime() {
        return timestamp.getMillis();
    }
}
