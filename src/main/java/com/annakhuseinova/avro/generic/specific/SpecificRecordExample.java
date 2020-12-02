package com.annakhuseinova.avro.generic.specific;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {

    public static void main(String[] args) {

        // Step 1: create specific record
        Customer.Builder customerBuilder = new Customer.Builder();
        customerBuilder.setFirstName("John");
        customerBuilder.setLastName("Doe");
        Customer customer = customerBuilder.build();
        System.out.println(customer);

        // Step 2: write to a file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try(DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)){
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
        } catch (IOException e){
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }

        // step 3: read from a file
        final File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new GenericDatumReader<>();
        Customer customerToRead;
        try(DataFileReader<Customer> dataFileReader = new DataFileReader<Customer>(file, datumReader)){
            customerToRead = dataFileReader.next();
            System.out.println(customerToRead);
            System.out.println("Customer name " + customerToRead.getFirstName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
