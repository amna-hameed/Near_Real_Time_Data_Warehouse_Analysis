package com.metro.utils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import com.metro.models.*;

public class CSVReader {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    // Helper method to clean currency values
    private static double parseCurrency(String value) {
        // Remove any currency symbols ($), commas, and trim whitespace
        String cleanValue = value.replaceAll("[$,]", "").trim();
        try {
            return Double.parseDouble(cleanValue);
        } catch (NumberFormatException e) {
            System.err.println("Error parsing currency value: " + value);
            throw e;
        }
    }
    
    // Get the project's root directory
    private static String getProjectPath() {
        File currentDir = new File(".");
        return currentDir.getAbsolutePath();
    }
    
    // Helper method to get file path
    private static String getFilePath(String filename) {
        String projectPath = getProjectPath();
        // Remove the trailing "." if present
        if (projectPath.endsWith(".")) {
            projectPath = projectPath.substring(0, projectPath.length() - 1);
        }
        return projectPath + "data" + File.separator + filename;
    }
    
    public static List<Transaction> readTransactions(String filename, int offset, int limit) {
        List<Transaction> transactions = new ArrayList<>();
        String filePath = getFilePath(filename);
        System.out.println("Reading transactions from: " + filePath);
        
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // Skip header
            br.readLine();
            
            // Skip offset
            for (int i = 0; i < offset; i++) {
                if (br.readLine() == null) break;
            }
            
            String line;
            int count = 0;
            while ((line = br.readLine()) != null && count < limit) {
                try {
                    String[] values = line.split(",");
                    if (values.length >= 5) {  // Verify we have enough columns
                        Transaction transaction = new Transaction(
                            Integer.parseInt(values[0].trim()),            // orderId
                            DATE_FORMAT.parse(values[1].trim()),          // orderDate
                            Integer.parseInt(values[2].trim()),           // productId
                            Integer.parseInt(values[4].trim()),           // customerId
                            Integer.parseInt(values[3].trim()),            // quantity
                            Integer.parseInt(values[5].trim())            // quantity
                        );
                        transactions.add(transaction);
                        count++;
                    } else {
                        System.err.println("Invalid line format: " + line);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing line: " + line);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error reading file: " + filePath);
            e.printStackTrace();
        }
        return transactions;
    }
    
    public static List<Customer> readCustomers(String filename, int offset, int limit) {
        List<Customer> customers = new ArrayList<>();
        String filePath = getFilePath(filename);
        System.out.println("Reading customers from: " + filePath);
        
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // Skip header
            br.readLine();
            
            // Skip offset
            for (int i = 0; i < offset; i++) {
                if (br.readLine() == null) break;
            }
            
            String line;
            int count = 0;
            while ((line = br.readLine()) != null && count < limit) {
                try {
                    String[] values = line.split(",");
                    if (values.length >= 3) {  // Verify we have enough columns
                        Customer customer = new Customer(
                            Integer.parseInt(values[0].trim()),    // customerId
                            values[1].trim(),                      // customerName
                            values[2].trim()                       // gender
                        );
                        customers.add(customer);
                        count++;
                    } else {
                        System.err.println("Invalid line format: " + line);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing line: " + line);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error reading file: " + filePath);
            e.printStackTrace();
        }
        return customers;
    }
    
    public static List<Product> readProducts(String filename, int offset, int limit) {
        List<Product> products = new ArrayList<>();
        String filePath = getFilePath(filename);
        System.out.println("Reading products from: " + filePath);
        
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // Skip header
            br.readLine();
            
            // Skip offset
            for (int i = 0; i < offset; i++) {
                if (br.readLine() == null) break;
            }
            
            String line;
            int count = 0;
            while ((line = br.readLine()) != null && count < limit) {
                try {
                    String[] values = line.split(",");
                    if (values.length >= 7) {  // Verify we have enough columns
                        Product product = new Product(
                            Integer.parseInt(values[0].trim()),    // productId
                            values[1].trim(),                      // productName
                            parseCurrency(values[2].trim()),       // productPrice - now using parseCurrency
                            Integer.parseInt(values[3].trim()),    // supplierId
                            values[4].trim(),                      // supplierName
                            Integer.parseInt(values[5].trim()),    // storeId
                            values[6].trim()                       // storeName
                        );
                        products.add(product);
                        count++;
                    } else {
                        System.err.println("Invalid line format: " + line);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing line: " + line);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error reading file: " + filePath);
            e.printStackTrace();
        }
        return products;
    }
}