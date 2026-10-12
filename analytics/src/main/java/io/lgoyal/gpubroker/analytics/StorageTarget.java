package io.lgoyal.gpubroker.analytics;

import java.net.URI;

/** Validates output roots before Hadoop FileSystem is contacted. */
public record StorageTarget(URI uri) {
    public StorageTarget {
        if (uri == null || uri.getScheme() == null) {
            throw new IllegalArgumentException("output root must have a URI scheme");
        }
        String scheme = uri.getScheme().toLowerCase();
        if (!(scheme.equals("file") || scheme.equals("hdfs") || scheme.equals("s3a"))) {
            throw new IllegalArgumentException("unsupported Hadoop output scheme: " + scheme);
        }
        if (scheme.equals("s3a") && (uri.getHost() == null || uri.getHost().isBlank())) {
            throw new IllegalArgumentException("s3a output root must include a bucket");
        }
    }

    public static StorageTarget parse(String value) {
        return new StorageTarget(URI.create(value));
    }
}
