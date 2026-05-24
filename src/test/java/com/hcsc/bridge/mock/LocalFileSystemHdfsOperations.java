package com.hcsc.bridge.mock;

import com.hcsc.bridge.hdfs.HdfsFileOperations;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class LocalFileSystemHdfsOperations implements HdfsFileOperations {

    private final Path baseDir;
    private final ConcurrentHashMap<String, AtomicBoolean> writtenFiles = new ConcurrentHashMap<>();
    private boolean shouldFailOnCreate = false;
    private boolean shouldFailOnRename = false;
    private String failureMessage = "Mock HDFS failure";

    public LocalFileSystemHdfsOperations(Path baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public boolean exists(String path) throws IOException {
        Path fullPath = resolvePath(path);
        return Files.exists(fullPath);
    }

    @Override
    public OutputStream create(String path) throws IOException {
        if (shouldFailOnCreate) {
            throw new IOException(failureMessage);
        }
        Path fullPath = resolvePath(path);
        Files.createDirectories(fullPath.getParent());
        writtenFiles.put(path, new AtomicBoolean(true));
        return new FileOutputStream(fullPath.toFile());
    }

    @Override
    public boolean rename(String sourcePath, String targetPath) throws IOException {
        if (shouldFailOnRename) {
            throw new IOException(failureMessage);
        }
        Path source = resolvePath(sourcePath);
        Path target = resolvePath(targetPath);

        if (!Files.exists(source)) {
            return false;
        }

        if (Files.exists(target)) {
            return false;
        }

        Files.createDirectories(target.getParent());
        Files.move(source, target);
        writtenFiles.put(targetPath, new AtomicBoolean(true));
        writtenFiles.remove(sourcePath);
        return true;
    }

    @Override
    public void delete(String path) throws IOException {
        Path fullPath = resolvePath(path);
        if (Files.exists(fullPath)) {
            Files.delete(fullPath);
        }
        writtenFiles.remove(path);
    }

    @Override
    public String getFileChecksum(String path) throws IOException {
        Path fullPath = resolvePath(path);
        if (!Files.exists(fullPath)) {
            throw new IOException("File not found: " + path);
        }

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (FileInputStream fis = new FileInputStream(fullPath.toFile())) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    md.update(buffer, 0, bytesRead);
                }
            }
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 algorithm not available", e);
        }
    }

    @Override
    public void mkdirs(String path) throws IOException {
        Path fullPath = resolvePath(path);
        Files.createDirectories(fullPath);
    }

    private Path resolvePath(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return baseDir.resolve(path);
    }

    public void setShouldFailOnCreate(boolean shouldFail) {
        this.shouldFailOnCreate = shouldFail;
    }

    public void setShouldFailOnRename(boolean shouldFail) {
        this.shouldFailOnRename = shouldFail;
    }

    public void setFailureMessage(String message) {
        this.failureMessage = message;
    }

    public boolean wasFileWritten(String path) {
        AtomicBoolean written = writtenFiles.get(path);
        return written != null && written.get();
    }

    public void reset() {
        shouldFailOnCreate = false;
        shouldFailOnRename = false;
        failureMessage = "Mock HDFS failure";
        writtenFiles.clear();
    }

    public void cleanup() throws IOException {
        if (Files.exists(baseDir)) {
            Files.walk(baseDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        writtenFiles.clear();
    }

    public Path getBaseDir() {
        return baseDir;
    }

    public String readFile(String path) throws IOException {
        Path fullPath = resolvePath(path);
        return Files.readString(fullPath);
    }
}
