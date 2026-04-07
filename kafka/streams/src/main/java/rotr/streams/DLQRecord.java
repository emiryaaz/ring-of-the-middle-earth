package rotr.streams;

public class DLQRecord {
    public String originalTopic;
    public int partition;
    public long offset;
    public String errorCode;
    public String errorMessage;
    public String rawPayload;
    public long timestamp;

    public DLQRecord() {
    }

    public DLQRecord(String originalTopic, int partition, long offset, String errorCode,
                     String errorMessage, String rawPayload, long timestamp) {
        this.originalTopic = originalTopic;
        this.partition = partition;
        this.offset = offset;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.rawPayload = rawPayload;
        this.timestamp = timestamp;
    }
}