package com.cwk.avrotest;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class NoCodeDeserializer {
	public static void main(String[] args) throws IOException {
		// First, we use a Parser to read our schema definition and create a Schema
		// object.

		Schema schema = new Schema.Parser().parse(NoCodeSerialzer.class.getClassLoader()
				.getResourceAsStream("user.avsc")/* .getClass().getResourceAsStream("./avro/user.avsc") */);

		// Deserialize users from disk
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("C:/Users/Roxy2.0/Desktop/users2.avro"), datumReader);
		GenericRecord user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}
}
