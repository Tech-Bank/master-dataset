package edu.prz.techbank.masterdataset.hdfs;

import edu.prz.techbank.masterdataset.exception.GeneralModuleException;
import java.io.IOException;
import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.hadoop.fs.*;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class HdfsService {

  final FileSystem fileSystem;

  public void uploadFile(String path, MultipartFile file) {
    Path hdfsPath = new Path(path);

    try (FSDataOutputStream outputStream = fileSystem.create(hdfsPath, true);
        InputStream inputStream = file.getInputStream()) {

      byte[] buffer = new byte[4096];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) > 0) {
        outputStream.write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      throw new GeneralModuleException("File upload error", e);
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

  public List<String> listStatus(String directory) {
    Path hdfsPath = new Path(directory);
    try {
      return Arrays.stream(fileSystem.listStatus(hdfsPath))
          .map(s -> s.getPath().toString() +
              (s.isDirectory() ? " [DIR]" : " [FILE]"))
          .toList();

    } catch (IOException e) {
      throw new GeneralModuleException("Status listing error", e);
    }
  }

  public List<String> listFiles(String directory, boolean recursive) {
    List<String> files = new ArrayList<>();
    Path hdfsPath = new Path(directory);

    try {
      val iterator = fileSystem.listFiles(hdfsPath, recursive);
      while (iterator.hasNext()) {
        files.add(iterator.next().toString());
      }
      return files;

    } catch (IOException e) {
      throw new GeneralModuleException("Files listing error", e);
    }
  }

  public boolean deleteFile(String path) {
    Path hdfsPath = new Path(path);
    try {
      return fileSystem.delete(hdfsPath, true);
    } catch (IOException e) {
      throw new GeneralModuleException("File deleting error", e);
    }
  }

}
