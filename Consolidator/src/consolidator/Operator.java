package consolidator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class Operator {
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String bucketName = "bucketfileconsolidator";
		
		Region region = Region.US_EAST_1;
		AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
		
		System.out.println("Getting the file from the bucket");
		
		S3Objects objects = S3Objects.inBucket(s3Client, bucketName);
		
		Consolidator consolidator = new Consolidator();
		System.out.println("Sending the file data to the Consolidator");
		consolidator.main(objects, bucketName);

		
	}
		

}
