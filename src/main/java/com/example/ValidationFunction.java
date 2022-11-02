package com.example;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.*;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Scanner;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;

public class ValidationFunction implements HttpFunction {
  @Override
  public void service(HttpRequest request, HttpResponse response) throws Exception {
    BufferedWriter writer = response.getWriter();
    writer.write("Hello world!");

    Storage storage = StorageOptions.getDefaultInstance().getService();
    BlobId blobId = BlobId.of("storage-test-alejandro","data.csv");
    Blob blob = storage.get(blobId);

    if(blobId != null){
      byte[] prevContent = blob.getContent();
      FileUtils.writeByteArrayToFile(new File("/tmp/data.csv"), prevContent);
      ArrayList<String> newFile = new ArrayList<String>();
      String newRow = "";
      Scanner scanner = new Scanner(new File("/tmp/data.csv"));
      if(scanner.hasNext())
      {
        String header = scanner.nextLine();
        header += ",flat";
        newFile.add(header);
      }
      while(scanner.hasNext()){
        newRow = scanner.nextLine();
        if(Integer.parseInt(newRow.split(",")[1]) > 30)
        {
          newFile.add(newRow += ", 1");
        }else{
          newFile.add(newRow += ", 2");
        }
      }

      Bucket newBucket = storage.create(BucketInfo.of("output-bucket-alejandro"));
      Bucket isValidBucket = storage.get("output-bucket-alejandro");
      String uploadingDate =  new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
      String finalFile = newFile.stream().map(Object::toString).collect(Collectors.joining("\n"));

      if(isValidBucket != null && isValidBucket.exists()){
        BlobId blobId1 = BlobId.of("output-bucket-alejandro", "data_updated"+ uploadingDate + ".csv");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId1).setContentType("text/plain").build();
        storage.create(blobInfo, finalFile.getBytes(StandardCharsets.UTF_8));
      }else {
        newBucket.create("data_updated"+ uploadingDate + ".csv", finalFile.getBytes(StandardCharsets.UTF_8));
      }
    }else{
      System.out.println("no encontro el blob");
    }
  }
}
