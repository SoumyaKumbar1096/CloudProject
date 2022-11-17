package workerLambda;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.CSVWriter;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;

import com.amazonaws.services.lambda.runtime.Context;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;


public class WorkerLambda implements RequestHandler<SNSEvent, String>{
	
	// handler function
	@Override
	public String handleRequest(SNSEvent event, Context context) {
		// TODO Auto-generated method stub
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
		context.getLogger().log("Invocation started: " + timeStamp);
		
		
		String[] message = event.getRecords().get(0).getSNS().getMessage().split(";");
		String bucketName = message[0];
		String fileKey = message[1];
		
		
		
		Region region = Region.US_EAST_1;
				
		
		System.out.println("Creating S3 Client");
		AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
		
		timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
		context.getLogger().log("[" + timeStamp +"]"+ "S3Client Created " );
			
		System.out.println("Retrieving "+ fileKey + " from "+ bucketName);
		//Retrieve file from the S3 bucket
		try(final S3Object s3Object = s3Client.getObject(bucketName, fileKey);
			final InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
			final BufferedReader reader = new BufferedReader(streamReader)) {
			
			String[] sampleDate = {""};
			String[] storeName = {""};
			Double[] totalProfit = {0.0};
			
			//Map to store total quantity per product
			Map<String, Integer> productQuantityList = new HashMap<String, Integer>();
			
			//Map to store total Profit per product
			Map<String , Double> productProfitList = new HashMap<String, Double>();
						
			//Map to store total sold per product
			Map<String, Double> productSoldList = new HashMap<String, Double>();
						
			timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
			context.getLogger().log("[" + timeStamp +"]"+ "Looping through " );
			
			reader.lines().skip(1).forEach(line -> {
				String[] rows = line.split(";");
				Integer rowLength = rows.length;

				//timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
				sampleDate[0] = rows[0];
				storeName[0] = rows[1];
				String product = rows[2];
				Integer quantity = Integer.parseInt(rows[3]);
				Double unitProfit = Double.parseDouble(rows[6]);
				Double totalPrice = Double.parseDouble(rows[7]);
				Double unitCost = Double.parseDouble(rows[5]);
								
				//Calculate total Profit of the store
				totalProfit[0] = totalProfit[0] + unitProfit;
				//Calculate Total quantity sold for each product
				if(!productQuantityList.containsKey(product)) {
					productQuantityList.put(product, quantity);
				} else {
					Integer newQuantity = productQuantityList.get(product) + quantity;
					productQuantityList.put(product, newQuantity);
				}
							
				//Calculate Total Profit earned for each Product
				if(!productProfitList.containsKey(product)) {
					productProfitList.put(product, unitProfit*quantity);
				} else {
					Double newUnitProfit = productProfitList.get(product) + (unitProfit*quantity);
					productProfitList.put(product, newUnitProfit);
				}
						
				//Calculate Total sold price for each Product
				if(!productSoldList.containsKey(product)) {
					productSoldList.put(product, unitCost*quantity);
				} else {
					Double newUnitCost = productSoldList.get(product) + (unitCost*quantity);
					productSoldList.put(product, newUnitCost);
				}
				
			});
			
			System.out.println("[" + timeStamp +"]"+ "Hash Maps populated " );
			
			String outputFilename = "Summary-" + fileKey ;
			
			//Write to the file
			CSVWriter csvWriter = new CSVWriter(new FileWriter("/tmp/"+outputFilename));
			System.out.println(csvWriter + ": CSV Writer is created");
			// Create 3 columns - product, Total quantity sold, Total Profit per product, Total Profit for the whole store
			String[] header = {"Date", "Store", "Product", "Total-QuantityPerProduct", "Total-SoldPerProduct", "Total-ProfitPerProduct", "Total-Profit"};
		
			System.out.println("Writing to summary csv file" );
			
			//Add header/columns to the csv file
			csvWriter.writeNext(header);
							
			//add data to the csv file
				String[] row = new String[7];
				row[0] = sampleDate[0];
				row[1] = storeName[0];
				//adding total profit of the store
				row[6] = Double.toString(totalProfit[0]);
							
				// Iterate over the Hash maps and add the values to the CSV files
				for(String key : productSoldList.keySet()) {
					row[2] = key;
						
					//adding total quantity value to the row String
					row[3] = Double.toString(productQuantityList.get(key)) ;
						
					//adding total sold value to the row String
					row[4] = Double.toString(productSoldList.get(key));
						
					//adding total profit per product
					row[5] = Double.toString(productProfitList.get(key));
						
					csvWriter.writeNext(row);
				}
									
				csvWriter.close();
				System.out.println("Writing to summary csv file");
				File file = new File("/tmp/"+outputFilename);
				s3Client.putObject("bucketfileconsolidator", "Summary-" + fileKey.substring(1), file);
				System.out.println("Summary file uploaded to " + bucketName);
			
		} catch(IOException e1) {
			e1.printStackTrace();
		}
		
		timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
		context.getLogger().log("Invocation completed: " + timeStamp);		
		return "OK";
	}

	
}
