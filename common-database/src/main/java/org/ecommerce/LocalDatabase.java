package org.ecommerce;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:/home/matheus/projetos/estudo-kafka-java/service-users/target/" + name + ".db";
        connection = DriverManager.getConnection(url);
    }

    public void createTableIfNotExists(String sql) {
        try{
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private PreparedStatement getPreparedStatement(String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i+1, params[i]);
        }
        return preparedStatement;
    }

    public boolean update(String statement, String ... params) throws SQLException {
        return getPreparedStatement(statement, params).execute();
    }

    public ResultSet query(String query, String ... params) throws SQLException {
        return getPreparedStatement(query, params).executeQuery();
    }

    public void close() throws SQLException {
        connection.close();
    }
}
