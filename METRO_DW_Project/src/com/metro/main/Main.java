// Main.java
package com.metro.main;

import java.sql.Connection;
import com.metro.utils.DatabaseConnection;
import com.metro.utils.MeshJoin;

public class Main {
    public static void main(String[] args) {
        try (Connection connection = DatabaseConnection.getConnection()) {
            System.out.println("Starting MESHJOIN process...");
            
            MeshJoin meshJoin = new MeshJoin(connection);
            meshJoin.executeJoin();
            
            System.out.println("MESHJOIN process completed successfully!");
        } catch (Exception e) {
            System.err.println("Error during MESHJOIN execution:");
            e.printStackTrace();
        }
    }
}