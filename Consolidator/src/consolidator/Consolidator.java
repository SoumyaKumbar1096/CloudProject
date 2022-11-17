package consolidator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Consolidator {
	
	public static void main(S3Objects objects, String bucketName) {
		
		// Consolidator receives data from the file as arguments
		System.out.println("Received data from the file");
		
		AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
		
		//List of Total-Profits of each store
		Map<String, Double> profitOfEachStoreList = new HashMap<String, Double>();
		
		//List of Total quantity sold of each product
		Map<String, Double> quantitySoldProductList = new HashMap<String, Double>();
		
		//List of Total profit made from each product
		Map<String, Double> profitEachProductList = new HashMap<String, Double>();
		
		//List of Total sold of each product
		Map<String, Double> soldEachProductList = new HashMap<String, Double>();
		
		Double[] totalRetailersProfit = {0.0} ;
		
		//Iterating through each file
		for(S3ObjectSummary obj: objects) {
			
			try(final S3Object s3Object = s3Client.getObject(bucketName, obj.getKey());
					final InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
					final BufferedReader reader = new BufferedReader(streamReader)) {
				
				System.out.println("Going through file " + s3Object.getKey());
				System.out.println(reader.readLine());
	
				
				//Iterating through contents of each file
				reader.lines().skip(1).forEach(line -> {
					String[] rows = line.split("\",\"");
					
					String storeName = rows[1];
					String product = rows[2];
					
					Double totalQuantity = Double.parseDouble(rows[3]);
					Double totalSold = Double.parseDouble(rows[4]);
					Double totalProfit = Double.parseDouble(rows[5]);
					try {
						Double totalProfitOfStore = Double.parseDouble(rows[6]);
						//List all total profits 
						if(!profitOfEachStoreList.containsKey(storeName)) {
							profitOfEachStoreList.put(storeName, totalProfitOfStore);
						} 
					} catch(NumberFormatException nfe) {
						nfe.printStackTrace();
					}
					
					
					//Store total quantity by each product
					if(!quantitySoldProductList.containsKey(product)) {
						quantitySoldProductList.put(product, totalQuantity);
					} else {
						Double newTotalQuantity = quantitySoldProductList.get(product) + totalQuantity;
						quantitySoldProductList.put(product, newTotalQuantity);
					}
					
					//Store total profit by each product
					if(!profitEachProductList.containsKey(product)) {
						profitEachProductList.put(product, totalProfit);
					} else {
						Double newTotalProfit = profitEachProductList.get(product) + totalProfit;
						profitEachProductList.put(product, newTotalProfit);
					}
					
					//Store total sold by each product
					if(!soldEachProductList.containsKey(product)) {
						soldEachProductList.put(product, totalSold);
					} else {
						Double newTotalSold = soldEachProductList.get(product)+ totalSold;
						soldEachProductList.put(product, newTotalSold);
					}	
				});
				
			} catch(IOException e1) {
				e1.printStackTrace();
			}
		}
		// And creates a new file 
		
			
			// Calculate Total retailer's profit and Display
		
		
			// calculate most profitable store
			// And Display
		Double maxProfit = 0.0;
		
		System.out.println(profitOfEachStoreList.size());
		
        for (Double value : profitOfEachStoreList.values()) {  // Iterate through HashMap
        	totalRetailersProfit[0] = totalRetailersProfit[0] + value;
            if(value > maxProfit) {
            	maxProfit = value;
            }
        }
        
        System.out.println("Total retailer's profit is : " + totalRetailersProfit[0]);
		for(String key: profitOfEachStoreList.keySet()) {
			if(profitOfEachStoreList.get(key) == maxProfit) {
				System.out.println(key + " : " + profitOfEachStoreList.get(key));
			}
		}
		
			// calculate least profitable store 
			// And Display
		
        System.out.println("Store(s) with least profit");
		Double minProfit = profitOfEachStoreList.get("store1") ;
		
		System.out.println(profitOfEachStoreList.size());
		
        for (Double value : profitOfEachStoreList.values()) {  // Iterate through HashMap
            if(value < minProfit) {
            	minProfit = value;
            }
        }
		System.out.println("Store(s) with most profit");
		for(String key: profitOfEachStoreList.keySet()) {
			if(profitOfEachStoreList.get(key) == minProfit) {
				System.out.println(key + " : " + profitOfEachStoreList.get(key));
			}
		}
		System.out.println(minProfit);

			// calculate total quantity per product
			// And Display
        System.out.println("Total Quantity sold per product");
        for(Entry<String, Double> entry : quantitySoldProductList.entrySet()) {
        	System.out.println(entry.getKey() + ":" + entry.getValue());
        }
			// calculate total sold per product
			// And Display
        System.out.println("Total sold per product");
        for(Entry<String, Double> entry : soldEachProductList.entrySet()) {
        	System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        
			// calculate total profit per product
			// And Display
        System.out.println("Total profit per product");
        for(Entry<String, Double> entry : profitEachProductList.entrySet()) {
        	System.out.println(entry.getKey() + ":" + entry.getValue());
        }
				
	}
	
	

}
