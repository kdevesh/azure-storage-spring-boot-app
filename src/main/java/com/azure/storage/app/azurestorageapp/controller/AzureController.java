package com.azure.storage.app.azurestorageapp.controller;

import com.azure.storage.app.azurestorageapp.config.AzureStorageConfiguration;
import com.azure.storage.app.azurestorageapp.dto.SasResponse;
import com.azure.storage.app.azurestorageapp.util.AzureUtil;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.UserDelegationKey;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * The type Azure controller.
 */
@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("azure/v1")
public class AzureController {
  /**
   * The Azure util.
   */
  AzureUtil azureUtil;
  /**
   * The Azure storage configuration.
   */
  AzureStorageConfiguration azureStorageConfiguration;

  private static final Logger logger = LoggerFactory.getLogger(AzureController.class);

  /**
   * Upload file response entity.
   *
   * @param file the file
   * @return the response entity
   */
  @PostMapping(path = "/upload", consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public ResponseEntity<String> uploadFile(@RequestParam(value = "file") MultipartFile file) {
    logger.info("Received file:{}", file.getOriginalFilename());
    try {
      azureUtil.uploadToAzureBlob(azureStorageConfiguration.getStorageContainer(), file.getOriginalFilename(), file.getBytes());
      return ResponseEntity.ok().body("Uploaded");
    } catch (IOException e) {
      logger.error("Exception occurred while uploading:{}:ex:{}", file.getOriginalFilename(),
          e.getMessage());
      return ResponseEntity.status(500).body(e.getMessage());
    }
  }

  /**
   * List files response entity.
   *
   * @param prefix the prefix
   * @return the response entity
   */
  @GetMapping(path = "/list/{prefix}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<BlobItem>> listFiles(@PathVariable String prefix) {
    logger.info("List files starting with:{}", prefix);
    try {
      List<BlobItem> blobItems = azureUtil.fetchReportsFromAzure(azureStorageConfiguration.getStorageContainer(), prefix);
      return ResponseEntity.ok().body(blobItems);
    } catch (Exception ex) {
      logger.error("Exception occurred:ex:{}", ex.getMessage());
      return ResponseEntity.status(500).body(Collections.emptyList());
    }
  }

  /**
   * Generate sas token response entity.
   *
   * @param fileName the file name
   * @return the response entity
   */
  @GetMapping(path = "/generate/sasToken/{fileName}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SasResponse> generateSasToken(@PathVariable String fileName) {
    logger.info("Generating SAS Token for:{}", fileName);
    SasResponse sasResponse = new SasResponse();
    try {
      UserDelegationKey userDelegationKey = azureUtil.generateUserDelegationKey();
      String sasUrl = azureUtil.generateSasToken(azureUtil.getBlobAsyncClient(azureStorageConfiguration.getStorageContainer(), fileName),
              userDelegationKey);
      sasResponse.setSasUrl(sasUrl);
      sasResponse.setFileName(fileName);
      sasResponse.setMessage("Generated");
      return ResponseEntity.ok().body(sasResponse);
    } catch (Exception ex) {
      logger.error("Exception occurred:ex:{}", ex.getMessage());
      sasResponse.setMessage(ex.getMessage());
      return ResponseEntity.status(500).body(sasResponse);
    }
  }

  /**
   * Download blob.
   *
   * @param fileName the file name
   * @param response the response
   */
  @GetMapping(path = "/download/{fileName:.+}",produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
  public void downloadBlob(@PathVariable String fileName, HttpServletResponse response) {
    logger.info("Downloading file:{}", fileName);
    OutputStream outputStream = null;
    try {
      response.setContentType("application/octet-stream");
      response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + fileName);
      outputStream = response.getOutputStream();
      azureUtil.downloadReportFromAzure(outputStream, azureStorageConfiguration.getStorageContainer(), fileName);
      outputStream.flush();
    } catch (Exception ex) {
      logger.error("Exception occurred:{}", ex.getMessage());
    } finally {
      if (outputStream != null) {
        IOUtils.closeQuietly(outputStream);
      }
    }
  }
}
