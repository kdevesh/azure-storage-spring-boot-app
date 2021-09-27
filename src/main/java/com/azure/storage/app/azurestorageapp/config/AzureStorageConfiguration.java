package com.azure.storage.app.azurestorageapp.config;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.nio.ByteBuffer;
import java.time.Duration;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;

/**
 * The type Azure storage configuration.
 */
@Data
@Configuration
@Slf4j
public class AzureStorageConfiguration {
  private static final Logger logger = LoggerFactory.getLogger(AzureStorageConfiguration.class);

  @Value("${app.config.azure.client-id}")
  private String clientId;
  @Value("${app.config.azure.client-secret}")
  private String clientSecret;
  @Value("${app.config.azure.tenant-id}")
  private String tenantId;
  @Value("${app.config.azure.storage-id}")
  private String storageId;
  @Value("${app.config.azure.storage-endpoint}")
  private String storageEndpoint;
  @Value("${app.config.azure.storage.container}")
  private String storageContainer;

  /**
   * Blob service client builder blob service client builder.
   *
   * @return the blob service client builder
   */
  @Bean
  public BlobServiceClientBuilder blobServiceClientBuilder() {
    return new BlobServiceClientBuilder()
        .credential(getAzureClientCredentials())
        .endpoint(getStorageEndpoint());
  }

  private ClientSecretCredential getAzureClientCredentials() {
    return new ClientSecretCredentialBuilder()
        .clientId(clientId)
        .clientSecret(clientSecret)
        .tenantId(tenantId)
        .build();
  }

  /**
   * Gets storage endpoint.
   *
   * @return the storage endpoint
   */
  public String getStorageEndpoint() {
    return storageEndpoint.replace("{STORAGE-ID}", storageId);
  }

  /**
   * A util method to upload a file to Azure Storage.
   *
   * @param blobServiceClientBuilder service client builder
   * @return BlobServiceAsyncClient blob service async client
   */
  @Bean(name = "blobServiceAsyncClient")
  public BlobServiceAsyncClient blobServiceAsyncClient(
      BlobServiceClientBuilder blobServiceClientBuilder) {
    /*
    retryDelay is by default 4ms and maxRetryDelay is by default 120ms
     */
    return blobServiceClientBuilder.retryOptions(
        new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            5,
            Duration.ofSeconds(300L),
            null,
            null,
            null)).buildAsyncClient();
  }

  /**
   * Blob service client blob service client.
   *
   * @param blobServiceClientBuilder the blob service client builder
   * @return the blob service client
   */
  @Bean(name = "blobServiceClient")
  public BlobServiceClient blobServiceClient(BlobServiceClientBuilder blobServiceClientBuilder) {
    /*
    retryDelay is by default 4ms and maxRetryDelay is by default 120ms
     */
    return blobServiceClientBuilder.retryOptions(
        new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            5,
            Duration.ofSeconds(300L),
            null,
            null,
            null)).buildClient();
  }

  /**
   * Covert byte array to flux flux.
   *
   * @param byteArray the byte array
   * @return the flux
   */
  public Flux<ByteBuffer> covertByteArrayToFlux(byte[] byteArray) {
    return Flux.just(ByteBuffer.wrap(byteArray));
  }

  /**
   * Creating TransferOptions.
   *
   * @param blockSize represents block size
   * @return ParallelTransferOptions transfer options
   */
  public ParallelTransferOptions getTransferOptions(long blockSize) {
    return new ParallelTransferOptions()
        .setBlockSizeLong(blockSize)
        .setMaxConcurrency(5)
        .setProgressReceiver(
            bytesTransferred -> logger.info("Uploading bytes:{}", bytesTransferred));
  }
}
