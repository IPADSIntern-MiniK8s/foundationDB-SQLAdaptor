package com.sjtu.se.ipads.fdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FdbApplication {

	public static void main(String[] args) {
		System.out.println("test success");
		FDB fdb = FDB.selectAPIVersion(710);

		try(Database db = fdb.open()) {
			// Run an operation on the database
			db.run(tr -> {
				tr.set(Tuple.from("hello").pack(), Tuple.from("world").pack());
				return null;
			});

			// Get the value of 'hello' from the database
			String hello = db.run(tr -> {
				byte[] result = tr.get(Tuple.from("hello").pack()).join();
				return Tuple.fromBytes(result).getString(0);
			});

			System.out.println("Hello " + hello);
		}
		SpringApplication.run(FdbApplication.class, args);
	}

}
