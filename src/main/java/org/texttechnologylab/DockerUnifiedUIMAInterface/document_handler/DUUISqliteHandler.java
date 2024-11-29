package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.sql.*;

public class DUUISqliteHandler {
    public static void main(String[] args) {
        String url = "jdbc:sqlite:TEMP_files/core.db";

        try (
                Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()
        ) {
            System.out.println("Connected to sqlite database");

            ResultSet rs = statement.executeQuery("SELECT * FROM users");
            while (rs.next()) {
                System.out.println(
                        "id: " + rs.getString("id")
                        + "\t username: " + rs.getString("username")
                        + "\t created: " + rs.getString("created")
                );
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}
