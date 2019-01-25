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

    private LogEntry() {
        this.timestamp = new DateTime(0);
    }

    public String client = "";
    public DateTime timestamp;
    public String httpMethod = "";
    public String resource = "";
    public String httpVersion = "";
    public int httpStatus;
    public int bytesTransferred;


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp).append(",");
        sb.append(client).append(",");
        sb.append(httpMethod).append(",");
        sb.append(resource).append(",");
        sb.append(httpVersion).append(",");
        sb.append(httpStatus).append(",");
        sb.append(bytesTransferred);
        return sb.toString();
    }

    public static LogEntry fromString(String line) {
        String regex = "(?<client>.*)\\s-\\s-\\s\\[(?<timestamp>.+)\\]\\s\"(?<httpMethod>.+)\\s(?<resource>.+)\\s?(?<httpVersion>HTTP/1.0)?\"\\s(?<httpStatus>\\d{3})\\s(?<bytesTransferred>\\d+)?";
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(line);
        LogEntry logEntry = new LogEntry();
        if (!matcher.find()) {
            logger.warn("Invalid record: " + line);
            return null;
        }

        logEntry.timestamp = DateTime.parse(matcher.group("timestamp"), timeFormatter);
        logEntry.client = matcher.group("client");
        logEntry.httpMethod = matcher.group("httpMethod");
        logEntry.resource = matcher.group("resource");
        logEntry.httpStatus = Integer.parseInt(matcher.group("httpStatus"));

        if (matcher.group("httpVersion") != null)
            logEntry.httpVersion = matcher.group("httpVersion");

        if (matcher.group("bytesTransferred") != null)
            logEntry.bytesTransferred = Integer.parseInt(matcher.group("bytesTransferred"));

        return logEntry;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LogEntry &&
                this.timestamp == ((LogEntry) other).timestamp
                && this.client.equals(((LogEntry) other).client)
                && this.bytesTransferred == ((LogEntry) other).bytesTransferred;
    }

    @Override
    public int hashCode() {
        return (int) this.timestamp.getMillis();
    }
}
