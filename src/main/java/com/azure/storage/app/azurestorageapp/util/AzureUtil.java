package com.azure.storage.app.azurestorageapp.util;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.Context;
import com.azure.storage.app.azurestorageapp.config.AzureStorageConfiguration;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.sas.SasProtocol;
import java.io.OutputStream;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The type Azure util.
 */
@Component
@AllArgsConstructor
public class AzureUtil {
  private static final Logger logger = LoggerFactory.getLogger(AzureUtil.class);

  /**
   * The Blob service client.
   */
  BlobServiceClient blobServiceClient;
  /**
   * The Blob service async client.
   */
  BlobServiceAsyncClient blobServiceAsyncClient;
  /**
   * The Azure storage configuration.
   */
  AzureStorageConfiguration azureStorageConfiguration;

  /**
   * Get blob container client blob container client.
   *
   * @param container the container
   * @return the blob container client
   */
  public BlobContainerClient getBlobContainerClient(String container) {
    return blobServiceClient.getBlobContainerClient(container);
  }

  /**
   * Fetch reports from azure list.
   *
   * @param container the container
   * @param prefix    the prefix
   * @return the list
   */
  public List<BlobItem> fetchReportsFromAzure(String container, String prefix) {
    try {
      Integer fetchPageSize = 1000;
      BlobContainerClient blobContainerClient = getBlobContainerClient(container);
      ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
      listBlobsOptions.setMaxResultsPerPage(fetchPageSize);
      listBlobsOptions.setPrefix(prefix);
      PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(listBlobsOptions, Duration.ofSeconds(60));
      Iterator<PagedResponse<BlobItem>> iterator = blobItems.iterableByPage().iterator();
      List<BlobItem> allBlobs = new ArrayList<>();
      String continuationToken;
      do {
        PagedResponse<BlobItem> pagedResponse = iterator.next();
        List<BlobItem> blobs = pagedResponse.getValue();
        continuationToken = pagedResponse.getContinuationToken();
        allBlobs.addAll(blobs);
      } while (continuationToken != null);
      return allBlobs;
    } catch (Exception ex) {
      logger.error("Exception occurred while fetching blob list from Azure:{}", ex.getMessage());
    }
    return Collections.emptyList();
  }

  /**
   * Download report from azure.
   *
   * @param outputStream the output stream
   * @param container    the container
   * @param blobName     the blob name
   */
  public void downloadReportFromAzure(OutputStream outputStream, String container,
                                      String blobName) {
    BlobClient blobClient = getBlobClient(container, blobName);
    blobClient.downloadStreamWithResponse(outputStream, null,
        new DownloadRetryOptions().setMaxRetryRequests(5),
        null, false, null, Context.NONE);
  }

  /**
   * Get blob client blob client.
   *
   * @param container the container
   * @param blobName  the blob name
   * @return the blob client
   */
  public BlobClient getBlobClient(String container, String blobName) {
    BlobContainerClient blobContainerClient = getBlobContainerClient(container);
    return blobContainerClient.getBlobClient(blobName);
  }

  /**
   * Upload to azure blob.
   *
   * @param container the container
   * @param blobName  the blob name
   * @param data      the data
   */
  public void uploadToAzureBlob(String container, String blobName, byte[] data) {
    BlobAsyncClient blobAsyncClient = getBlobAsyncClient(container, blobName);
    long blockSize = 2L * 1024L * 1024L; //2MB
    blobAsyncClient.upload(azureStorageConfiguration.covertByteArrayToFlux(data),
            azureStorageConfiguration.getTransferOptions(blockSize), true)
        .doOnSuccess(blockBlobItem -> logger.info("Successfully uploaded !!"))
        .doOnError(throwable -> logger.error(
            "Error occurred while uploading !! Exception:{}",
            throwable.getMessage()))
        .subscribe();
  }

  /**
   * Get blob async client blob async client.
   *
   * @param container the container
   * @param blobName  the blob name
   * @return the blob async client
   */
  public BlobAsyncClient getBlobAsyncClient(String container, String blobName) {
    BlobContainerAsyncClient blobContainerAsyncClient =
        blobServiceAsyncClient.getBlobContainerAsyncClient(container);
    return blobContainerAsyncClient.getBlobAsyncClient(blobName);
  }

  /**
   * Generate user delegation key user delegation key.
   *
   * @return the user delegation key
   */
  public UserDelegationKey generateUserDelegationKey(){
    OffsetDateTime keyStart = OffsetDateTime.now();
    OffsetDateTime keyExpiry = OffsetDateTime.now().plusDays(5);
    return blobServiceAsyncClient.getUserDelegationKey(keyStart, keyExpiry)
        .doOnError(throwable -> logger.error("Failed to create User Delegation Key:{}",
            throwable.getMessage()))
        .doOnSuccess(userDelegationKey -> {
          logger.info("UserDelegationKey created:{}", Objects.nonNull(userDelegationKey));
        }).block();
  }

  /**
   * Generate sas token string.
   *
   * @param blobAsyncClient   the blob async client
   * @param userDelegationKey the user delegation key
   * @return the string
   */
  public String generateSasToken(BlobAsyncClient blobAsyncClient,
                                  UserDelegationKey userDelegationKey) {
    String sasUrl = null;
    BlobContainerSasPermission blobContainerSasPermission = new BlobContainerSasPermission()
        .setReadPermission(true);
    BlobServiceSasSignatureValues builder = new BlobServiceSasSignatureValues(
        userDelegationKey.getSignedExpiry(), blobContainerSasPermission)
        .setProtocol(SasProtocol.HTTPS_ONLY);
    try {
      sasUrl = String.format("%s/%s/%s?%s", azureStorageConfiguration.getStorageEndpoint(),
          blobAsyncClient.getContainerName(),
          blobAsyncClient.getBlobName(),
          blobAsyncClient.generateUserDelegationSas(builder, userDelegationKey));
    } catch (Exception ex) {
      logger.error("Exception occurred while creating SAS Url:{}", ex.getMessage());
    }
    return sasUrl;
  }
}
