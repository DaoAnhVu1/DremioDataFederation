package org.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        final String DB_URL = "jdbc:dremio:direct=localhost:31010;";
        final String USER = "daoanhvu";
        final String PASS = "daoanhvu123";
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASS);

        Connection conn = null;
        Statement stmt = null;
        try {
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, props);

            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT * FROM \"top_100_ratings\".\"result\";";
            System.out.println("Executing statement...");
            ResultSet rs = stmt.executeQuery(sql);

            System.out.println("Printing the result in table format:");
            System.out.println("--------------------------------------");

            // Print column headers
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            TableGenerator tableGenerator = new TableGenerator();
            List<String> headersList = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                headersList.add(metaData.getColumnName(i));
            }


            List<List<String>> rowsList = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getString(i));
                }
                rowsList.add(row);
            }

            System.out.println(tableGenerator.generateTable(headersList, rowsList));

            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
}
