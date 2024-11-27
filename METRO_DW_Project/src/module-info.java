module METRO_DW_Project {
    requires java.sql;
    requires mysql.connector.j;
    requires java.base;
    
    // If you want to use Scanner, also add:
    requires java.desktop;
    
    // Opens your packages to java.sql
    opens com.metro.utils to java.sql;
    opens com.metro.models to java.sql;
}