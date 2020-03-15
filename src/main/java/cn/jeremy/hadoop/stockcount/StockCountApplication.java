package cn.jeremy.hadoop.stockcount;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class StockCountApplication {

	public static void main(String[] args) {
		SpringApplication.run(StockCountApplication.class, args);
	}

}
