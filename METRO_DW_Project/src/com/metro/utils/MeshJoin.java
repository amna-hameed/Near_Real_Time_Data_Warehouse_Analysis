package com.metro.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import com.metro.models.*;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Calendar;

public class MeshJoin {
    private final int DISK_BUFFER_SIZE = 100000;
    private final int STREAM_BUFFER_SIZE = 10000;
    private final Connection connection;
    private final Queue<Transaction> queue;
    private final Map<Integer, List<Transaction>> hashTable;
    private final List<Customer> customerBuffer;
    private final List<Product> productBuffer;
    
    private int transactionOffset = 0;
    private final String TRANSACTION_FILE = "transactions_data.csv";
    private final String CUSTOMER_FILE = "customers_data.csv";
    private final String PRODUCT_FILE = "products_data.csv";
    
    public MeshJoin(Connection connection) {
        this.connection = connection;
        this.queue = new LinkedList<>();
        this.hashTable = new HashMap<>();
        this.customerBuffer = new ArrayList<>(DISK_BUFFER_SIZE);
        this.productBuffer = new ArrayList<>(DISK_BUFFER_SIZE);
    }
    
    public void executeJoin() throws Exception {
        int customerPartitionIndex = 0;
        int productPartitionIndex = 0;
        
        // Start transaction
        connection.setAutoCommit(false);
        
        try {
            // First load all dimension data
            loadAllDimensionData();
            
            while (true) {
                // Step 1: Read stream segment
                List<Transaction> streamSegment = readTransactionSegment();
                if (streamSegment.isEmpty()) {
                    break; // No more transactions to process
                }
                
                System.out.println("Processing " + streamSegment.size() + " transactions...");
                
                // Add to hash table and queue
                for (Transaction transaction : streamSegment) {
                    hashTable.computeIfAbsent(transaction.getCustomerId(), k -> new ArrayList<>())
                            .add(transaction);
                    queue.offer(transaction);
                }
                
                // Step 2: Load next master data partitions
                loadCustomerPartition(customerPartitionIndex);
                loadProductPartition(productPartitionIndex);
                
                // Step 3: Perform join and generate output
                processJoin();
                
                // Update partition indices
                customerPartitionIndex = (customerPartitionIndex + 1) % getTotalCustomerPartitions();
                productPartitionIndex = (productPartitionIndex + 1) % getTotalProductPartitions();
                
                // Remove processed transactions
                removeProcessedTransactions();
                
                // Commit batch
                connection.commit();
                
                System.out.println("Batch processed successfully.");
            }
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    private void loadAllDimensionData() throws SQLException {
        // Load Suppliers first
        loadSuppliers();
        
        // Load Stores next
        loadStores();
        
        // Load Products last (depends on Suppliers and Stores)
        loadProducts();
        
        // Load Customers
        loadCustomers();
        
        // Commit dimension data
        connection.commit();
    }
    
    private void loadSuppliers() throws SQLException {
        String insertSupplierSql = "INSERT IGNORE INTO Suppliers_data (SupplierID, SupplierName) VALUES (?, ?)";
        Set<Integer> processedSuppliers = new HashSet<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader("data/" + PRODUCT_FILE))) {
            String line = br.readLine(); // Skip header
            
            try (PreparedStatement pstmt = connection.prepareStatement(insertSupplierSql)) {
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    int supplierId = Integer.parseInt(parts[3].trim());
                    
                    if (!processedSuppliers.contains(supplierId)) {
                        pstmt.setInt(1, supplierId);
                        pstmt.setString(2, parts[4].trim());
                        pstmt.addBatch();
                        processedSuppliers.add(supplierId);
                    }
                }
                pstmt.executeBatch();
            }
        } catch (Exception e) {
            throw new SQLException("Error loading suppliers: " + e.getMessage());
        }
    }
    
    private void loadStores() throws SQLException {
        String insertStoreSql = "INSERT IGNORE INTO Stores_data (StoreID, StoreName) VALUES (?, ?)";
        Set<Integer> processedStores = new HashSet<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader("data/" + PRODUCT_FILE))) {
            String line = br.readLine(); // Skip header
            
            try (PreparedStatement pstmt = connection.prepareStatement(insertStoreSql)) {
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    int storeId = Integer.parseInt(parts[5].trim());
                    
                    if (!processedStores.contains(storeId)) {
                        pstmt.setInt(1, storeId);
                        pstmt.setString(2, parts[6].trim());
                       
                        pstmt.addBatch();
                        processedStores.add(storeId);
                    }
                }
                pstmt.executeBatch();
            }
        } catch (Exception e) {
            throw new SQLException("Error loading stores: " + e.getMessage());
        }
    }
    
    private void loadProducts() throws SQLException {
        String insertProductSql = "INSERT IGNORE INTO products_data (ProductID, ProductName, ProductPrice, SupplierID,SupplierName, StoreID,StoreName) VALUES (?, ?, ?, ?, ?,?,?)";
        
        try (BufferedReader br = new BufferedReader(new FileReader("data/" + PRODUCT_FILE))) {
            String line = br.readLine(); // Skip header
            
            try (PreparedStatement pstmt = connection.prepareStatement(insertProductSql)) {
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    pstmt.setInt(1, Integer.parseInt(parts[0].trim()));
                    pstmt.setString(2, parts[1].trim());
                    pstmt.setDouble(3, Double.parseDouble(parts[2].trim()));
                    pstmt.setInt(4, Integer.parseInt(parts[3].trim()));
                    pstmt.setString(5, parts[4].trim());
                    pstmt.setInt(6, Integer.parseInt(parts[5].trim()));
                    pstmt.setString(7, parts[6].trim());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        } catch (Exception e) {
            throw new SQLException("Error loading products: " + e.getMessage());
        }
    }
    
    private void loadCustomers() throws SQLException {
        String insertCustomerSql = "INSERT IGNORE INTO customers_data (CustomerID, CustomerName, Gender) VALUES (?, ?, ?)";
        
        try (BufferedReader br = new BufferedReader(new FileReader("data/" + CUSTOMER_FILE))) {
            String line = br.readLine(); // Skip header
            
            try (PreparedStatement pstmt = connection.prepareStatement(insertCustomerSql)) {
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    pstmt.setInt(1, Integer.parseInt(parts[0].trim()));
                    pstmt.setString(2, parts[1].trim());
                    pstmt.setString(3, parts[2].trim());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        } catch (Exception e) {
            throw new SQLException("Error loading customers: " + e.getMessage());
        }
    }
    private List<Transaction> readTransactionSegment() {
        return CSVReader.readTransactions(TRANSACTION_FILE, transactionOffset, STREAM_BUFFER_SIZE);
    }
    private void loadCustomerPartition(int partitionIndex) {
        customerBuffer.clear();
        customerBuffer.addAll(CSVReader.readCustomers(CUSTOMER_FILE, partitionIndex * DISK_BUFFER_SIZE, DISK_BUFFER_SIZE));
    }
    
    private void loadProductPartition(int partitionIndex) {
        productBuffer.clear();
        productBuffer.addAll(CSVReader.readProducts(PRODUCT_FILE, partitionIndex * DISK_BUFFER_SIZE, DISK_BUFFER_SIZE));
    }
    
    private int getOrCreateDateId(Date orderDate) throws SQLException {
        Calendar cal = Calendar.getInstance();
        cal.setTime(orderDate);

        // Check if date exists
        String selectSql = "SELECT DateID FROM Dates_data WHERE FullDate = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
            pstmt.setDate(1, new java.sql.Date(orderDate.getTime()));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("DateID");
                }
            }
        }

        // Insert new date
        String insertSql = "INSERT INTO Dates_data (FullDate, Year, Quarter, Month, MonthName, Day, DayOfWeek, " +
            "DayName, IsWeekend, Season) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setDate(1, new java.sql.Date(orderDate.getTime()));
            pstmt.setInt(2, cal.get(Calendar.YEAR));
            pstmt.setInt(3, (cal.get(Calendar.MONTH) / 3) + 1);
            pstmt.setInt(4, cal.get(Calendar.MONTH) + 1);
            pstmt.setString(5, getMonthName(cal.get(Calendar.MONTH)));
            pstmt.setInt(6, cal.get(Calendar.DAY_OF_MONTH));
            pstmt.setInt(7, cal.get(Calendar.DAY_OF_WEEK));
            pstmt.setString(8, getDayName(cal.get(Calendar.DAY_OF_WEEK)));
            pstmt.setBoolean(9, isWeekend(cal.get(Calendar.DAY_OF_WEEK)));
            pstmt.setString(10, getSeason(cal.get(Calendar.MONTH)));

            pstmt.executeUpdate();

            // Get the generated ID using proper try-with-resources
            try (ResultSet rs = pstmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                throw new SQLException("Failed to get generated DateID");
            }
        }
    }

    private void processJoin() throws SQLException {
        String insertSql = "INSERT Ignore INTO Sales_data " +
            "(order_id, DateID, ProductID, CustomerID, Quantity, Sale, StoreID, SupplierID) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
            for (Customer customer : customerBuffer) {
                List<Transaction> transactions = hashTable.get(customer.getCustomerId());
                if (transactions != null) {
                    for (Transaction transaction : transactions) {
                        for (Product product : productBuffer) {
                            if (product.getProductId() == transaction.getProductId()) {
                                double totalSale = product.getProductPrice() * transaction.getQuantity();

                                insertStmt.setInt(1, transaction.getOrderId());
                                insertStmt.setInt(2, getOrCreateDateId(transaction.getOrderDate()));
                                insertStmt.setInt(3, product.getProductId());
                                insertStmt.setInt(4, customer.getCustomerId());
                                insertStmt.setInt(5, transaction.getQuantity());
                                insertStmt.setDouble(6, totalSale);
                                insertStmt.setInt(7, product.getStoreId());
                                insertStmt.setInt(8, product.getSupplierId());
                                insertStmt.addBatch();
                            }
                        }
                    }
                }
            }
            insertStmt.executeBatch();
        }
    }
    // Helper method to ensure dimension data is loaded before processing transactions
    private void loadDimensionData() throws SQLException {
        // Load Suppliers
        String insertSupplierSql = "INSERT IGNORE INTO Suppliers_data (SupplierID, SupplierName) VALUES (?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSupplierSql)) {
            for (Product product : productBuffer) {
                pstmt.setInt(1, product.getSupplierId());
                pstmt.setString(2, product.getSupplierName());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        
        // Load Stores
        String insertStoreSql = "INSERT IGNORE INTO Stores_data (StoreID, StoreName) VALUES (?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertStoreSql)) {
            for (Product product : productBuffer) {
                pstmt.setInt(1, product.getStoreId());
                pstmt.setString(2, product.getStoreName());
                
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        
        // Load Products
        String insertProductSql = "INSERT IGNORE INTO products_data (ProductID, ProductName, ProductPrice, SupplierID,SupplierName, StoreID, StoreName) VALUES (?, ?, ?, ?, ?, ?,?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertProductSql)) {
            for (Product product : productBuffer) {
                pstmt.setInt(1, product.getProductId());
                pstmt.setString(2, product.getProductName());
                pstmt.setDouble(3, product.getProductPrice());
                pstmt.setInt(4, product.getSupplierId());
                pstmt.setString(5, product.getSupplierName());
                pstmt.setInt(6, product.getStoreId());
                pstmt.setString(5, product.getStoreName());
                
             
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    // Call this method at the start of executeJoin()
    
   
    private String getMonthName(int month) {
        return new String[]{"January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"}[month];
    }
    
    private String getDayName(int dayOfWeek) {
        return new String[]{"Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday"}[dayOfWeek - 1];
    }
    
    private boolean isWeekend(int dayOfWeek) {
        return dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY;
    }
    
    private String getSeason(int month) {
        if (month <= 1 || month == 11) return "Winter";
        if (month <= 4) return "Spring";
        if (month <= 7) return "Summer";
        return "Fall";
    }
    
    private void removeProcessedTransactions() {
        while (queue.size() > STREAM_BUFFER_SIZE) {
            Transaction oldestTransaction = queue.poll();
            List<Transaction> transactions = hashTable.get(oldestTransaction.getCustomerId());
            if (transactions != null) {
                transactions.remove(oldestTransaction);
                if (transactions.isEmpty()) {
                    hashTable.remove(oldestTransaction.getCustomerId());
                }
            }
        }
    }
    
    private int getTotalCustomerPartitions() throws Exception {
        int totalCustomers = 0;
        try (BufferedReader br = new BufferedReader(new FileReader("data/" + CUSTOMER_FILE))) {
            br.readLine(); // Skip header
            while (br.readLine() != null) totalCustomers++;
        }
        return (int) Math.ceil((double) totalCustomers / DISK_BUFFER_SIZE);
    }
    
    private int getTotalProductPartitions() throws Exception {
        int totalProducts = 0;
        try (BufferedReader br = new BufferedReader(new FileReader("data/" + PRODUCT_FILE))) {
            br.readLine(); // Skip header
            while (br.readLine() != null) totalProducts++;
        }
        return (int) Math.ceil((double) totalProducts / DISK_BUFFER_SIZE);
    }
}