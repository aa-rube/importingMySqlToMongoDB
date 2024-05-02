package app;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;


public class DataMigration {
    public static void main(String[] args) {

        String mySQLURL = "jdbc:mysql://localhost:3306/antispam";
        String mySQLUser = "root";
        String mySQLPassword = "30STMParadise($)_(&)";

        String mongoURL = "mongodb://localhost:27017";


        try {
            Connection connection = DriverManager.getConnection(mySQLURL, mySQLUser, mySQLPassword);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM admins");

            MongoDatabase mongoDatabase = MongoClients.create(mongoURL).getDatabase("antispam");
            MongoCollection<Document> collection = mongoDatabase.getCollection("admins");

            List<Document> documents = new ArrayList<>();
            while (resultSet.next()) {
                Document doc = new Document("id", resultSet.getString("id"))
                        .append("name", resultSet.getString("name"));
                documents.add(doc);
            }

            if (!documents.isEmpty()) {
                collection.insertMany(documents);
            }

            System.out.println("Данные успешно перенесены!");

            resultSet.close();
            statement.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
