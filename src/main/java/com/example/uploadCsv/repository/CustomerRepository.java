package com.example.uploadCsv.repository;

import com.example.uploadCsv.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer,Long> {
}
