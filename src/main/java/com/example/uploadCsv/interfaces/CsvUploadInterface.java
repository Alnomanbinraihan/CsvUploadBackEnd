package com.example.uploadCsv.interfaces;

import org.springframework.web.multipart.MultipartFile;

public interface CsvUploadInterface {
    String uploadCsv(MultipartFile file);
}
