package com.enterprise.bridge.model;

import java.util.Objects;

public final class HdfsWriteResult {

    private final String hdfsPath;
    private final String checksum;
    private final long bytesWritten;
    private final boolean alreadyExists;

    private HdfsWriteResult(String hdfsPath, String checksum, long bytesWritten, boolean alreadyExists) {
        this.hdfsPath = hdfsPath;
        this.checksum = checksum;
        this.bytesWritten = bytesWritten;
        this.alreadyExists = alreadyExists;
    }

    public static HdfsWriteResult success(String hdfsPath, String checksum, long bytesWritten) {
        return new HdfsWriteResult(
                Objects.requireNonNull(hdfsPath, "hdfsPath must not be null"),
                Objects.requireNonNull(checksum, "checksum must not be null"),
                bytesWritten,
                false
        );
    }

    public static HdfsWriteResult alreadyExists(String hdfsPath, String checksum) {
        return new HdfsWriteResult(
                Objects.requireNonNull(hdfsPath, "hdfsPath must not be null"),
                Objects.requireNonNull(checksum, "checksum must not be null"),
                0,
                true
        );
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public String getChecksum() {
        return checksum;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public boolean isAlreadyExists() {
        return alreadyExists;
    }

    public boolean isNewWrite() {
        return !alreadyExists;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HdfsWriteResult that = (HdfsWriteResult) o;
        return bytesWritten == that.bytesWritten &&
                alreadyExists == that.alreadyExists &&
                Objects.equals(hdfsPath, that.hdfsPath) &&
                Objects.equals(checksum, that.checksum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hdfsPath, checksum, bytesWritten, alreadyExists);
    }

    @Override
    public String toString() {
        return "HdfsWriteResult{" +
                "hdfsPath='" + hdfsPath + '\'' +
                ", checksum='" + checksum + '\'' +
                ", bytesWritten=" + bytesWritten +
                ", alreadyExists=" + alreadyExists +
                '}';
    }
}
