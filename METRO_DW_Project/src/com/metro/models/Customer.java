package com.metro.models;

public class Customer {
    private int customerId;
    private String customerName;
    private String gender;
    
    // Constructor
    public Customer(int customerId, String customerName, String gender) {
        this.customerId = customerId;
        this.customerName = customerName;
        this.gender = gender;
    }
    
    // Getters and Setters
    public int getCustomerId() { return customerId; }
    public void setCustomerId(int customerId) { this.customerId = customerId; }
    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }
    public String getGender() { return gender; }
    public void setGender(String gender) { this.gender = gender; }
}
