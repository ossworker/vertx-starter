package com.workoss.boot.tomcat;


import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@EnableCaching
@SpringBootApplication
public class VertxServerSpringBootExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(VertxServerSpringBootExampleApplication.class, args);
    }


    @GetMapping("/ping")
    public String ok(){
        return "pong";
    }
}
