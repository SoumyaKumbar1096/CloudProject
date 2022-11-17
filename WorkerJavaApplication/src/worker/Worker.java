package worker;



	

import java.io.BufferedReader;
import java.io.File;
	import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.CSVWriter;

import software.amazon.awssdk.core.ResponseBytes;
	import software.amazon.awssdk.regions.Region;
	import software.amazon.awssdk.services.s3.S3Client;
	import software.amazon.awssdk.services.s3.model.GetObjectRequest;
	import software.amazon.awssdk.services.s3.model.GetObjectResponse;
	import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
	import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

import software.amazon.awssdk.services.sqs.SqsClient;
	import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
	import software.amazon.awssdk.services.sqs.model.Message;
	import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class Worker{

  public static void main(String[] args) {
	  		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
	  		System.out.println("Invocation started : " + "[" + timestamp +"]");
			Region region = Region.US_EAST_1;


			String queueURL = "https://sqs.us-east-1.amazonaws.com/887675141876/clientMessageQueue";

			SqsClient sqsClient = SqsClient.builder().region(region).build();

			ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueURL).maxNumberOfMessages(1)
					.build();

			List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

			if (!messages.isEmpty()) {
				Message msg = messages.get(0);

				String[] arguments = msg.body().split(";");
				String bucketName = arguments[0];
				String fileName = arguments[1];

				AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
				
					
					try(final S3Object s3Object = s3.getObject(bucketName, fileName);
							final InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
							final BufferedReader reader = new BufferedReader(streamReader)) {
							
							String[] headerLine = reader.readLine().split(";");
							String[] sampleDate = {""};
							String[] storeName = {""};
							Double[] totalProfit = {0.0};
										
							//Map to store total quantity per product
							Map<String, Integer> productQuantityList = new HashMap<String, Integer>();
							
							//Map to store total Profit per product
							Map<String , Double> productProfitList = new HashMap<String, Double>();
										
							//Map to store total sold per product
							Map<String, Double> productSoldList = new HashMap<String, Double>();
										

							reader.lines().forEach(line -> {
								String[] rows = line.split(";");
								sampleDate[0] = rows[0];
								storeName[0] = rows[1];
								String product = rows[2];
								Integer quantity = Integer.parseInt(rows[3]);
								Double unitProfit = Double.parseDouble(rows[6]);
								Double totalPrice = Double.parseDouble(rows[7]);
								Double unitCost = Double.parseDouble(rows[5]);
												
								//Calculate total Profit of the store
								totalProfit[0] = totalProfit[0] + (unitProfit * quantity);
								
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
							
							
							String outputFilename = "Summary-" + fileName ;
							
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
								s3.putObject("bucketfileconsolidator", "Summary-" + fileName.substring(1), file);
								System.out.println("Summary file uploaded to " + bucketName);
							
							
						} catch(IOException e1) {
							e1.printStackTrace();
						}

					// Delete the message
					for (Message message : messages) {
						DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueURL)
								.receiptHandle(message.receiptHandle()).build();

						sqsClient.deleteMessage(deleteMessageRequest);
					}
				} else {
					System.out.println("File is not available in the Bucket");
				}
			
			timestamp = new Timestamp(System.currentTimeMillis());
	  		System.out.println("Invocation completed : " + "[" + timestamp +"]");
			
		}
	}