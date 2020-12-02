package com.annakhuseinova.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordExamples {
    public static void main(String[] args) {
        // step 0: define a schema
        Schema.Parser parser = new Schema.Parser();
        // Can parse from a string or a file
        Schema schema = parser.parse("" +
                "{\n" +
                "\t\"type\": \"record\",\n" +
                "\t\"namespace\": \"com.example\",\n" +
                "\t\"name\": \"customer\",\n" +
                "\t\"doc\": \"AvroSchema for our customer\"\n" +
                "\t\"fields\": [\n" +
                "\n" +
                "\t\t{\"name\": \"first_name\", \"type\":\"string\", \"doc\": \"First name of the customer\"},\n" +
                "\t\t{\"name\": \"last_name\", \"type\": \"string\", \"doc\":\"Last name of the customer\"},\n" +
                "\t\t{\"name\": \"automated_email\", \"type\":\"boolean\", \"default\": true, \"doc\":\"true if should send marketing emails\"}\n" +
                "\t]\n" +
                "}");
        // step 1. Create a generic record
        GenericRecordBuilder customBuilder = new GenericRecordBuilder(schema);
        customBuilder.set("first_name", "John");
        customBuilder.set("last_name", "Doe");
        GenericData.Record customer = customBuilder.build();
        System.out.println(customer);
        // step 2. Write that generic record to a file.
        // writing to a file
        // DatumWriter - the object that is going to write to a schema.
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)){
            dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
            dataFileWriter.append(customer);
        } catch (IOException e){
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }

        // step 3. Reading a generic record from a file.
        final File file = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord genericRecord;
        try(DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader)){
            genericRecord = dataFileReader.next();
            System.out.println(genericRecord);
            System.out.println("Customer name " + genericRecord.get("first_name"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
