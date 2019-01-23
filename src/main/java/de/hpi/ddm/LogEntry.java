package de.hpi.ddm;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.log4j.Logger;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogEntry {
    private final static Logger logger = Logger.getLogger(LogEntry.class);

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z").withLocale(Locale.US).withZoneUTC();

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


    public static LogEntry fromString(String line) {
        String regex = "(?<accessor>.*)\\s-\\s-\\s\\[(?<timestamp>.+)\\]\\s\"(?<httpMethod>.+)\\s(?<resource>.+)\\s(?<httpVersion>.+)\"\\s(?<httpStatus>\\d{3})\\s(?<bytesTransferred>\\d+)";
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(line);
        if (!matcher.find() ||  matcher.groupCount() != 7) {
            logger.warn("Invalid record: " + line);
            return new LogEntry();
        }

        LogEntry logEntry = new LogEntry();

        try {
            logEntry.timestamp = DateTime.parse(matcher.group("timestamp"), timeFormatter);
            logEntry.accessor = matcher.group("accessor");
            logEntry.httpMethod = matcher.group("httpMethod");
            logEntry.resource = matcher.group("resource");
            logEntry.httpVersion = matcher.group("httpVersion");
            logEntry.httpStatus = Integer.parseInt(matcher.group("httpStatus"));
            logEntry.bytesTransferred = Integer.parseInt(matcher.group("bytesTransferred"));
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
