package com.metro.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

public class DatabaseConnection {
    private static Connection connection = null;
    private static String url;
    private static String username;
    private static String password;

    public static void initializeConnection() {
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("Enter database URL (e.g., jdbc:mysql://localhost:3306/metro_dw): ");
        url = scanner.nextLine();
        
        System.out.println("Enter database username: ");
        username = scanner.nextLine();
        
        System.out.println("Enter database password: ");
        password = scanner.nextLine();
        
        try {
            // Register JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            
            // Open a connection
            System.out.println("Connecting to database...");
            connection = DriverManager.getConnection(url, username, password);
            
            if (connection != null) {
                System.out.println("Database connected successfully!");
            }
            
        } catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC Driver not found.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Connection failed! Check output console");
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        if (connection == null) {
            initializeConnection();
        }
        return connection;
    }

    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
                System.out.println("Database connection closed.");
            } catch (SQLException e) {
                System.out.println("Error closing connection!");
                e.printStackTrace();
            }
        }
    }
}