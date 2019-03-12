package com.cwk.avrotest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.NewCookie;

public class ConvertStream2Record {

	static String schema = "{\"namespace\": \"com.dsg.avro\",\r\n" + " \"type\": \"record\",\r\n"
			+ " \"name\": \"User\",\r\n" + " \"fields\": [\r\n" + "     {\"name\": \"name\", \"type\": \"string\"},\r\n"
			+ "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\r\n"
			+ "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\r\n" + " ]\r\n" + "}";
	static String jsonString = "[{\"name\": \"zxcxz锴\", \"favorite_number\": 8388, \"favorite_color\": null},"
			+ "{\"name\": \"里相fgf互\", \"favorite_number\": 999, \"favorite_color\": \"red\"}]";
	
	static String jsonString1 = "[{\"name\": \"asdasd11锴\", \"favorite_number\": 888, \"favorite_color\": null},"
			+ "{\"name\": \"里asdas111互\", \"favorite_number\": 999, \"favorite_color\": \"red\"}]";

	// 我们需要什么？
	// 1.schema
	// 2.json数据
	@SuppressWarnings("unlikely-arg-type")
	public static void main(String[] args) throws IOException {

		/*
		 * Schema schema = new Schema.Parser()
		 * .parse(ConvertStream2Record.class.getClassLoader().getResourceAsStream(
		 * "user.avsc"));
		 */

		Schema inferedAvroSchema = inferedAvroSchema(jsonString);
		convertStreamToRecord(inferedAvroSchema);

		System.out.println(inferedAvroSchema.toString(true));

		/*
		 * Object aObject=true; System.out.println(aObject.getClass().getSimpleName());
		 */
	}

	/**
	 * 初步达成在jsonstream中抽取出Record
	 * 
	 * @throws IOException
	 */
	public static void convertStreamToRecord(Schema infSchema) throws IOException {
		Schema schema = infSchema;
				//new Schema.Parser().parse(ConvertStream2Record.schema);
		File file = new File("C:\\Users\\Roxy2.0\\Desktop\\users2.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, file);

		ByteArrayInputStream bis = new ByteArrayInputStream(jsonString1.getBytes());

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		while (bis.available() > 0) {
			int read = bis.read();
			bos.write(read); //
			System.out.println("read: " + read + "bis.available()" + bis.available());
		}

		JSONArray parseArray = JSON.parseArray(new String(bos.toByteArray()));

		Object json = JSON.toJSON(parseArray);

		// System.out.println(json);

		final GenericRecord record = new GenericData.Record(schema);

		List<Schema.Field> fields = schema.getFields();
		Iterator<Object> iterator = parseArray.iterator();
		while (iterator.hasNext()) {
			JSONObject object = (JSONObject) iterator.next();
			for (int j = 0; j < fields.size(); j++) {
				record.put(j, object.get(fields.get(j).name()));
			}
			dataFileWriter.append(record);
		}

		dataFileWriter.close();
	}

	/**
	 * 这是在做什么？ 我需要从json数组或者单条json数据中推断出avro的schema信息
	 * 
	 * @param jsonString
	 * @return
	 */
	public static Schema inferedAvroSchema(String jsonString) {

		JSONArray parseArray = JSON.parseArray(jsonString);

		JSONObject object = (JSONObject) parseArray.get(1);

		Map<String, Object> innerMap = object.getInnerMap();

		SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("table").fields();

		for (Map.Entry<String, Object> entry : innerMap.entrySet()) {
			try {
				System.out.println("name: " + entry.getValue().getClass().getSimpleName());
			} catch (Exception e) {
				System.out.println(entry.getKey() + "			" + e.getMessage());
			}

			switch (entry.getValue().getClass().getSimpleName()) {
			case "Integer":
				builder.name(entry.getKey()).type().unionOf().nullBuilder().endNull().and().intType().endUnion()
						.noDefault();
				break;
			case "String":
				builder.name(entry.getKey()).type().unionOf().nullBuilder().endNull().and().stringType().endUnion()
						.noDefault();
				break;
			case "Long":
				builder.name(entry.getKey()).type().unionOf().nullBuilder().endNull().and().longType().endUnion()
						.noDefault();
				break;
			case "Double":
				builder.name(entry.getKey()).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion()
						.noDefault();
				break;
			case "Boolean":
				builder.name(entry.getKey()).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion()
						.noDefault();
				break;
			case "byte[]":
				builder.name(entry.getKey()).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion()
						.noDefault();
				break;
			default:
				throw new IllegalArgumentException(
						"createSchema: Unknown data type " + entry.getValue() + " cannot be converted to Avro type");
			}

		}
		return builder.endRecord();
		/*
		 * Schema schema =
		 * .name("clientHash").type().fixed("MD5").size(16).noDefault().name(
		 * "clientProtocol").type().nullable()
		 * .stringType().noDefault().name("serverHash").type("MD5").name("meta").type().
		 * nullable().map().values() .bytesType().noDefault().endRecord();
		 */

	}

}
