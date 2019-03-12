package com.cwk.avrotest;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

public class PerformanceTest {

	private static int max = 10000000;

	public static void main(String[] args) throws IOException {

		javaSerializer();

		writableSerializer();

		avroSerializer();

	}

	/**
	 * java内存序列化
	 * 
	 * @throws IOException
	 */
	private static void javaSerializer() throws IOException {

		long start = System.currentTimeMillis();

		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		ObjectOutputStream oos = new ObjectOutputStream(bos);

		MyUser user = null;

		for (int i = 0; i < max; i++) {
			user = new MyUser("a" + i, i, "c" + i);
			// user.setName();
			// user.setFavorite_number(i);
			// user.setFavorite_color("c" + i);
			oos.writeObject(user);
		}
		oos.close();
		System.out.println(
				"time collape:" + (System.currentTimeMillis() - start) + "\t size:" + bos.toByteArray().length);

	}

	private static void writableSerializer() throws IOException {

		long start = System.currentTimeMillis();

		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		DataOutputStream dos = new DataOutputStream(bos);

		MyUser user = null;

		for (int i = 0; i < max; i++) {
			dos.writeUTF("a" + i);
			dos.writeInt(i);
			dos.writeUTF("c" + i);
			// user.setName();
			// user.setFavorite_number(i);
			// user.setFavorite_color("c" + i);
		}
		dos.close();
		System.out.println(
				"time collape:" + (System.currentTimeMillis() - start) + "\t size:" + bos.toByteArray().length);

	}

	private static void avroSerializer() throws IOException {
		long start = System.currentTimeMillis();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		Schema schema = new Schema.Parser().parse(NoCodeSerialzer.class.getClassLoader()
				.getResourceAsStream("user.avsc")/* .getClass().getResourceAsStream("./avro/user.avsc") */);

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, bos);
		GenericRecord user = null;
		for (int i = 0; i < max; i++) {
			user = new GenericData.Record(schema);
			user.put("name", "a" + i);
			user.put("favorite_number", i);
			dataFileWriter.append(user);
		}

		dataFileWriter.close();
		System.out.println(
				"time collape:" + (System.currentTimeMillis() - start) + "\t size:" + bos.toByteArray().length);
	}

}
