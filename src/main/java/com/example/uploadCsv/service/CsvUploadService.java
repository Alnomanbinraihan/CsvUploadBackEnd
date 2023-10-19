package com.example.uploadCsv.service;

import com.example.uploadCsv.entity.Customer;
import com.example.uploadCsv.interfaces.CsvUploadInterface;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStreamReader;

import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class CsvUploadService implements CsvUploadInterface {

    public boolean isCSVFile(MultipartFile file) {
        String contentType = file.getContentType();
        return contentType != null && contentType.equals("text/csv");
    }

    public CopyOnWriteArrayList<Customer> convertCsvToCustomerList(MultipartFile file) {
        CopyOnWriteArrayList<Customer> customerList = new CopyOnWriteArrayList<>();

        try (InputStreamReader reader = new InputStreamReader(file.getInputStream())) {
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

            csvParser.getRecords().parallelStream()
                    .forEach(csvRecord -> {
                        Customer customer = new Customer();

                        customer.setName(csvRecord.get("Name"));
                        customer.setAddress(csvRecord.get("Address"));
                        customer.setEmail(csvRecord.get("Email"));

                        customerList.add(customer);
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }

        return customerList;
    }


    @Override
    public String uploadCsv(MultipartFile file) {

        boolean isCsvFile = isCSVFile(file);

        if (isCsvFile) {
            CopyOnWriteArrayList<Customer> customers = convertCsvToCustomerList(file);
            if (customers.size() > 0) {

            }

            return "CSV file uploaded successfully.";
        } else {
            return "Please provide CSV file";
        }

    }
}
