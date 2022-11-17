package Client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Scanner;

import org.joda.time.DateTime;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class Client {
	
	public static void main(String[] args) {
		// Application uploads the daily file into the Cloud  storage(S3 bucket)
		Region region= Region.US_EAST_1;

		String bucketName = "bucketsk12";
		String filePath = "C://Users/hp/Documents/CPS2/Sem 3/Cloud and Edge/sales-data-upload";
		String fileName = "01-10-2022-store2.csv";
		String topicARN = "arn:aws:sns:us-east-1:887675141876:ClientFileUploaded";
        String queueURL = "https://sqs.us-east-1.amazonaws.com/887675141876/clientMessageQueue";
		
//		Scanner sc= new Scanner(System.in); 
//	    System.out.println("Please insert your bucketName:" ); 
//	    String str= sc.nextLine();  
//	    String bucketName = str;
//	    
//	    System.out.println("Please insert your path:" ); 
//	    String str2= sc.nextLine(); 
//	    String filePath =  str2;
//	    
//	    System.out.println("Please insert your filename:" ); 
//	    String str3= sc.nextLine(); 
//	    String fileName =  str3;
//	    
//	    System.out.println("Please insert your topicARN:" ); 
//	    String str4= sc.nextLine(); 
//	    String topicARN =  str4;
//		
//	    System.out.println("Please insert your queueURL:" ); 
//	    String str5= sc.nextLine(); 
//	    String queueURL =  str5;
        
		uploadFile(region, bucketName, filePath, fileName);
		
		// Publish to the topic(Use SNS service to notify worker application)
		//notifyWorker(region, bucketName, fileName, topicARN);
		
		//Send an SQS message
		sendSQS(region, bucketName, fileName, queueURL);	
	}
	
	public static void uploadFile(Region region, String bucketName, String filePath, String fileName) {
		
		S3Client s3 = S3Client.builder().region(region).build();
		
		ListBucketsRequest listBucketRequest = ListBucketsRequest.builder().build();
		
		ListBucketsResponse listBucketResponse = s3.listBuckets(listBucketRequest);
		
		//If there are no buckets with the bucketName then create a new one
		if((listBucketResponse.hasBuckets()) && (listBucketResponse.buckets()
				.stream().noneMatch( x -> x.name().equals(bucketName)))) {
			
			CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
					.bucket(bucketName).build();
			
			s3.createBucket(bucketRequest);
		}
		
		//To upload a file
		PutObjectRequest putOb = PutObjectRequest.builder().bucket(bucketName)
				.key(fileName).build();
		s3.putObject(putOb, 
				RequestBody.fromBytes(getObjectFile(filePath + File.separator + fileName)));
		
		System.out.println("The file is uploaded");
		
	}
	
	private static byte[] getObjectFile(String filePath) {

	    FileInputStream fileInputStream = null;
	    byte[] bytesArray = null;

	    try {
	      File file = new File(filePath);
	      bytesArray = new byte[(int) file.length()];
	      fileInputStream = new FileInputStream(file);
	      fileInputStream.read(bytesArray);

	    } catch (IOException e) {
	      e.printStackTrace();
	    } finally {
	      if (fileInputStream != null) {
	        try {
	          fileInputStream.close();
	        } catch (IOException e) {
	          e.printStackTrace();
	        }
	      }
	    }
	    return bytesArray;
	  }
	
	public static boolean notifyWorker(Region region, String bucketName, String fileName, String topicARN) {
		
		try {
			SnsClient snsClient = SnsClient.builder().region(region).build();

			PublishRequest request = PublishRequest.builder().message(bucketName + ";" + fileName).topicArn(topicARN)
					.build();

			PublishResponse snsResponse = snsClient.publish(request);
			System.out.println(
					snsResponse.messageId() + " Message sent. Status is " + snsResponse.sdkHttpResponse().statusCode());

			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			System.out.println("Notified the Worker Lambda at : " + "[" + timestamp +"]");
			
		} catch (SnsException e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
		return false;
	}
	
	public static void sendSQS(Region region, String bucketName, String fileName, String queueURL) {
		
		SqsClient sqsClient = SqsClient.builder().region(region).build();

		SendMessageRequest sendRequest = SendMessageRequest.builder().queueUrl(queueURL)
				.messageBody(bucketName + ";" + fileName).build();

		SendMessageResponse sqsResponse = sqsClient.sendMessage(sendRequest);

		System.out.println(
				sqsResponse.messageId() + " Message sent. Status is " + sqsResponse.sdkHttpResponse().statusCode());
		
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		System.out.println("Notified the Worker Java application at : " + "[" + timestamp +"]");
	}
}
