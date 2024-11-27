drop schema if exists `metroDB` ;
create schema `metroDB` ;
use metroDB;
drop table if exists `customers_data`;
create table customers_data (
    CustomerID int primary key,
    CustomerName VARCHAR(100) not null,
    Gender CHAR(1) CHECK (Gender IN ('M', 'F'))
);
drop table if exists `products_data`;
create table products_data (
    ProductID int primary key,
    ProductName VARCHAR(100) not null,
    ProductPrice decimal(10, 2) not null,
    SupplierID int not null,
	SupplierName varchar(100) not null,
    StoreID int not null,
	StoreName varchar(100) not null
);
drop table if exists `Suppliers_data`;
create table Suppliers_data (
    SupplierID int primary key,
    SupplierName VARCHAR(100) not null
);
drop table if exists `Stores_data`;
create table Stores_data (
    StoreID int primary key,
    StoreName VARCHAR(100) not null
  
);
drop table if exists `Dates_data`;
create table Dates_data (
	DateID INT AUTO_INCREMENT PRIMARY KEY,
    FullDate DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(10) NOT NULL,
    Day INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName VARCHAR(10) NOT NULL,
    IsWeekend BOOLEAN NOT NULL,
    Season VARCHAR(10) NOT NULL
);
drop table if exists `Sales_data`;
create table Sales_data (
    order_id int primary key,
    DateID int not null,
    ProductID int not null,
    CustomerID int not null,
    Quantity int not null,
    Sale DECIMAL(10, 2) not null,
    StoreID int not null,
    SupplierID int not null,
    FOREIGN key (DateID) references Dates_data(DateID),
    FOREIGN key (ProductID) references products_data(ProductID),
    FOREIGN key (CustomerID) references customers_data(CustomerID),
    FOREIGN key (StoreID) references Stores_data(StoreID),
    FOREIGN key (SupplierID) references Suppliers_data(SupplierID)
);
