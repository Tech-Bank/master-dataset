package edu.prz.techbank.masterdataset.hdfs;

import org.apache.hadoop.fs.*;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@Service
public class HdfsService {

  private final FileSystem fileSystem;

  public HdfsService(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  public void uploadFile(String path, MultipartFile file) throws Exception {
    Path hdfsPath = new Path(path);

    try (FSDataOutputStream outputStream = fileSystem.create(hdfsPath, true);
        InputStream inputStream = file.getInputStream()) {

      byte[] buffer = new byte[4096];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) > 0) {
        outputStream.write(buffer, 0, bytesRead);
      }
    }
  }

  public void downloadFile(String path, OutputStream outputStream) throws Exception {
    Path hdfsPath = new Path(path);

    try (FSDataInputStream inputStream = fileSystem.open(hdfsPath)) {
      byte[] buffer = new byte[4096];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) > 0) {
        outputStream.write(buffer, 0, bytesRead);
      }
    }
  }

  public List<String> listFiles(String directory) throws Exception {
    List<String> files = new ArrayList<>();
    Path hdfsPath = new Path(directory);

    FileStatus[] fileStatuses = fileSystem.listStatus(hdfsPath);
    for (FileStatus status : fileStatuses) {
      files.add(status.getPath().toString() +
          (status.isDirectory() ? " [DIR]" : " [FILE]"));
    }
    return files;
  }

  public boolean deleteFile(String path) throws Exception {
    Path hdfsPath = new Path(path);
    return fileSystem.delete(hdfsPath, true); // true = recursive
  }
}
