package rotr.streams;

public class PathStateRecord {
    public String pathId;
    public boolean blocked;
    public long timestamp;

    public PathStateRecord() {
    }

    public PathStateRecord(String pathId, boolean blocked, long timestamp) {
        this.pathId = pathId;
        this.blocked = blocked;
        this.timestamp = timestamp;
    }
}
