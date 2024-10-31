import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

import org.json.JSONObject;
import org.json.JSONArray;

public class GetData {

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database
    String userTableName = null;
    String friendsTableName = null;
    String cityTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;

    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
        super();
        String dataType = u;
        oracleConnection = c;
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        cityTableName = prefix + dataType + "_CITIES";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITIES";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITIES";
    }

    // TODO: Implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException {

        // This is the data structure to store all users' information
        JSONArray users_info = new JSONArray();
        
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            String query = "SELECT u.user_id, u.first_name, u.last_name, u.gender, u.year_of_birth, u.month_of_birth, u.day_of_birth, "
                + "c1.city_name AS current_city, c1.state_name AS current_state, c1.country_name AS current_country, "
                + "c2.city_name AS hometown_city, c2.state_name AS hometown_state, c2.country_name AS hometown_country "
                + "FROM " + userTableName + " u, " + currentCityTableName + " cc, " + cityTableName + " c1, " + hometownCityTableName + " hc, " + cityTableName + " c2 "
                + "WHERE u.user_id = cc.user_id "
                + "AND c1.city_id = cc.current_city_id "
                + "AND u.user_id = hc.user_id "
                + "AND c2.city_id = hc.hometown_city_id "
                + "ORDER BY u.user_id";
            
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                JSONObject currentUser = new JSONObject();
                int userId = rs.getInt("user_id");
                currentUser.put("user_id", rs.getInt("user_id"));
                currentUser.put("first_name", rs.getString("first_name"));
                currentUser.put("last_name", rs.getString("last_name"));
                currentUser.put("gender", rs.getString("gender"));
                currentUser.put("YOB", rs.getInt("year_of_birth"));
                currentUser.put("MOB", rs.getInt("month_of_birth"));
                currentUser.put("DOB", rs.getInt("day_of_birth"));

                JSONObject currentCity = new JSONObject();
                currentCity.put("city", rs.getString("current_city"));
                currentCity.put("state", rs.getString("current_state"));
                currentCity.put("country", rs.getString("current_country"));
                currentUser.put("current", currentCity);

                JSONObject hometown = new JSONObject();
                hometown.put("city", rs.getString("hometown_city"));
                hometown.put("state", rs.getString("hometown_state"));
                hometown.put("country", rs.getString("hometown_country"));
                currentUser.put("hometown", hometown);
                
                JSONArray friendsArray = new JSONArray();
                
                try (Statement friendStmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                    String friendsQuery = "SELECT user2_id FROM " + friendsTableName 
                                        + " WHERE user1_id = " + userId + " AND user2_id > " + userId;
                    ResultSet friendRs = friendStmt.executeQuery(friendsQuery);
                    while (friendRs.next()) {
                        friendsArray.put(friendRs.getInt("user2_id"));
                    }
                    friendRs.close();
                }
                currentUser.put("friends", friendsArray);
                users_info.put(currentUser);
            }
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return users_info;
    }

    // This outputs to a file "output.json"
    // DO NOT MODIFY this function
    public void writeJSON(JSONArray users_info) {
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir") + "/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
