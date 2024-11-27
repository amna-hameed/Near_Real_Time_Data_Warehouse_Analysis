package com.metro.models;

import java.util.Date;

public class Transaction {
    private int order_id;
    private Date orderDate;
    private int productId;
    private int customerId;
    private int quantity;
    private int DateID;
    
    // Constructor
    public Transaction(int orderId, Date orderDate, int productId, int customerId, int quantity, int DateID) {
        this.order_id = orderId;
        this.orderDate = orderDate;
        this.productId = productId;
        this.customerId = customerId;
        this.quantity = quantity;
        this.DateID = DateID;
    }
    
    // Getters and Setters
    public int getOrderId() { return order_id; }
    public void setOrderId(int orderId) { this.order_id = orderId; }
    public Date getOrderDate() { return orderDate; }
    public void setOrderDate(Date orderDate) { this.orderDate = orderDate; }
    public int getProductId() { return productId; }
    public void setProductId(int productId) { this.productId = productId; }
    public int getCustomerId() { return customerId; }
    public void setCustomerId(int customerId) { this.customerId = customerId; }
    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
    public int getDateID() { return DateID; }
    public void setDateID(int DateID) { this.DateID = DateID; }
}