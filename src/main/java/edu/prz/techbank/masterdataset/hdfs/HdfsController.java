package edu.prz.techbank.masterdataset.hdfs;

import edu.prz.techbank.masterdataset.exception.GeneralModuleException;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/hdfs")
@RequiredArgsConstructor
public class HdfsController {

  final HdfsService hdfsService;

  @PostMapping
  public ResponseEntity<String> uploadFile(@RequestParam("path") String path,
      @RequestParam("file") MultipartFile file) {

    hdfsService.uploadFile(path, file);

    return ResponseEntity.ok().build();
  }

  @GetMapping
  public ResponseEntity<Void> downloadFile(@RequestParam("path") String path,
      HttpServletResponse response) {

    response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
        "attachment; filename=\"" + path.replace('/', '-') + "\"");
    response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);

    try {
      hdfsService.downloadFile(path, response.getOutputStream());
      response.flushBuffer();

      return ResponseEntity.ok().build();

    } catch (Exception e) {
      throw new GeneralModuleException("File download error ", e);
    }
  }

  @GetMapping("/list-status")
  public ResponseEntity<List<String>> listStatus(@RequestParam("directory") String directory) {

    List<String> files = hdfsService.listStatus(directory);
    return ResponseEntity.ok(files);
  }

  @GetMapping("/list-files")
  public ResponseEntity<List<String>> listFiles(@RequestParam("directory") String directory) {

    List<String> files = hdfsService.listFiles(directory, false);
    return ResponseEntity.ok(files);
  }

  @GetMapping("/list-files-recursive")
  public ResponseEntity<List<String>> listTree(@RequestParam("directory") String directory) {

    List<String> files = hdfsService.listFiles(directory, true);
    return ResponseEntity.ok(files);
  }

  @DeleteMapping
  public ResponseEntity<Void> deleteFile(@RequestParam("path") String path) {

    boolean deleted = hdfsService.deleteFile(path);
    if (deleted) {
      return ResponseEntity.ok().build();
    } else {
      return ResponseEntity.notFound().build();
    }
  }

}
