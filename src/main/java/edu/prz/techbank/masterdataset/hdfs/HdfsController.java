package edu.prz.techbank.masterdataset.hdfs;

import edu.prz.techbank.masterdataset.exception.GeneralModuleException;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import jakarta.servlet.http.HttpServletResponse;
import java.util.List;

@RestController
@RequestMapping("/hdfs")
@RequiredArgsConstructor
public class HdfsController {

  private final HdfsService hdfsService;

  @PostMapping("/upload")
  public ResponseEntity<String> uploadFile(
      @RequestParam("file") MultipartFile file,
      @RequestParam("path") String path) {

    hdfsService.uploadFile(path, file);

    return ResponseEntity.ok().build();
  }

  @GetMapping("/download")
  public ResponseEntity<Void> downloadFile(
      @RequestParam("path") String path,
      HttpServletResponse response) {

    response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + path + "\"");
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
    try {
      List<String> files = hdfsService.listFiles(directory, false);
      return ResponseEntity.ok(files);
    } catch (Exception e) {
      return ResponseEntity.internalServerError().build();
    }
  }

  @GetMapping("/list-files-recursive")
  public ResponseEntity<List<String>> listTree(@RequestParam("directory") String directory) {
    try {
      List<String> files = hdfsService.listFiles(directory, true);
      return ResponseEntity.ok(files);
    } catch (Exception e) {
      return ResponseEntity.internalServerError().build();
    }
  }

  @DeleteMapping("/delete")
  public ResponseEntity<String> deleteFile(@RequestParam("path") String path) {
    try {
      boolean deleted = hdfsService.deleteFile(path);
      if (deleted) {
        return ResponseEntity.ok("Usunięto: " + path);
      } else {
        return ResponseEntity.status(404).body("Nie znaleziono pliku: " + path);
      }
    } catch (Exception e) {
      return ResponseEntity.internalServerError().body("Błąd: " + e.getMessage());
    }
  }

}
