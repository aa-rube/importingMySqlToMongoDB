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
            ResultSet resultSet = statement.executeQuery("SELECT * FROM advertisers_users");

            MongoDatabase mongoDatabase = MongoClients.create(mongoURL).getDatabase("antispam");
            MongoCollection<Document> collection = mongoDatabase.getCollection("advertisers_users;");

            List<Document> documents = new ArrayList<>();
            while (resultSet.next()) {
                Document doc = new Document(
                        "admin_chat_id_owner", resultSet.getString("admin_chat_id_owner"))
                        .append("admin_user_name_owner", resultSet.getString("admin_user_name_owner"))
                        .append("days_of_permission", resultSet.getString("days_of_permission"))
                        .append("end_permission", resultSet.getString("end_permission"))
                        .append("permission_to_group", resultSet.getString("permission_to_group"))
                        .append("post_count", resultSet.getString("post_count"))
                        .append("user_name", resultSet.getString("user_name"))
                        .append("started", resultSet.getString("started")
                        );
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
