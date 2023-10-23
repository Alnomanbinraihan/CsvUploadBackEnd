package com.example.uploadCsv.service;

import com.example.uploadCsv.entity.Customer;
import com.example.uploadCsv.interfaces.CsvUploadInterface;
import com.example.uploadCsv.repository.CustomerRepository;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

@Service
public class CsvUploadService implements CsvUploadInterface {

    @Autowired
    CustomerRepository customerRepository;

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


    public void saveDataInParallel(CopyOnWriteArrayList<Customer> dataToSave, int batchSize, int threadCount) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threadCount);
        executor.setMaxPoolSize(threadCount);
        executor.initialize();

        List<List<Customer>> dataBatches = splitIntoBatches(dataToSave, batchSize);
        CountDownLatch latch = new CountDownLatch(dataBatches.size());

        for (List<Customer> batch : dataBatches) {
            executor.execute(() -> {
                saveBatch(batch);
                latch.countDown();
            });
        }


        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.shutdown();
    }

    private List<List<Customer>> splitIntoBatches(CopyOnWriteArrayList<Customer> data, int batchSize) {
        List<List<Customer>> batches = new ArrayList<>();
        for (int i = 0; i < data.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, data.size());
            batches.add(data.subList(i, endIndex));
        }
        return batches;
    }

    private void saveBatch(List<Customer> dataBatch) {
        customerRepository.saveAll(dataBatch);
    }

    @Override
    public String uploadCsv(MultipartFile file) {

        boolean isCsvFile = isCSVFile(file);

        if (isCsvFile) {
            CopyOnWriteArrayList<Customer> customers = convertCsvToCustomerList(file);
            if (customers.size() > 0) {
                saveDataInParallel(customers, 1000000, 10);
            }
            return "CSV file uploaded successfully.";
        } else {
            return "Please provide CSV file";
        }

    }
}
