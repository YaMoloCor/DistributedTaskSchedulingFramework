package org.example.job.admin;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class JobAdminApplication {
    public static void main(String[] args) {
        SpringApplication.run(JobAdminApplication.class, args);
    }
}
