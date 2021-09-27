package com.azure.storage.app.azurestorageapp.dto;

import lombok.Data;

@Data
public class SasResponse {
  private String fileName;
  private String sasUrl;
  private String message;
}
