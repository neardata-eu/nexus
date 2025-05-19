package io.nexus.streamlets.utils;

import java.io.Serializable;
import java.util.Objects;

public class FastqRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private long id;               // Sequential position in the FASTQ file
    private String header;         // The line starting with '@'
    private String sequence;       // DNA/RNA sequence
    private String optionalHeader; // Line starting with '+', usually optional
    private String quality;        // ASCII-encoded quality scores

    public FastqRecord() {
        // Required for Flink POJO serialization
    }

    public FastqRecord(long id, String header, String sequence, String optionalHeader, String quality) {
        this.id = id;
        this.header = header;
        this.sequence = sequence;
        this.optionalHeader = optionalHeader;
        this.quality = quality;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getSequence() {
        return sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    public String getOptionalHeader() {
        return optionalHeader;
    }

    public void setOptionalHeader(String optionalHeader) {
        this.optionalHeader = optionalHeader;
    }

    public String getQuality() {
        return quality;
    }

    public void setQuality(String quality) {
        this.quality = quality;
    }

    @Override
    public String toString() {
        return String.format("FastqRecord{id=%d, header='%s'}", id, header);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FastqRecord)) return false;
        FastqRecord that = (FastqRecord) o;
        return id == that.id &&
                Objects.equals(header, that.header) &&
                Objects.equals(sequence, that.sequence) &&
                Objects.equals(optionalHeader, that.optionalHeader) &&
                Objects.equals(quality, that.quality);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, header, sequence, optionalHeader, quality);
    }
}

