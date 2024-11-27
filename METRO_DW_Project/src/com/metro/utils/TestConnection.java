package com.metro.utils;

import java.sql.Connection;
import java.sql.SQLException;

public class TestConnection {
    public static void main(String[] args) {
        try {
            // Get database connection
            Connection conn = DatabaseConnection.getConnection();
            
            if (conn != null) {
                System.out.println("Connection test successful!");
                
                // Test if connection is valid
                if (conn.isValid(5)) {  // timeout of 5 seconds
                    System.out.println("Connection is valid and active");
                    
                    // Print database information
                    System.out.println("Database Product: " + conn.getMetaData().getDatabaseProductName());
                    System.out.println("Database Version: " + conn.getMetaData().getDatabaseProductVersion());
                }
                
                // Close the connection
                DatabaseConnection.closeConnection();
            }
            
        } catch (SQLException e) {
            System.out.println("Error testing connection!");
            e.printStackTrace();
        }
    }
}