package edu.jsu.mcis.cs310;

import java.sql.*;
import org.json.simple.*;
import org.json.simple.parser.*;

public class Database {
    
    private final Connection connection;
    
    private final int TERMID_SP22 = 1;
    
    /* CONSTRUCTOR */

    public Database(String username, String password, String address) {
        
        this.connection = openConnection(username, password, address);
        
    }
    
    /* PUBLIC METHODS */

    public String getSectionsAsJSON(int termid, String subjectid, String num) {
        
        String result = null;
        
        // New query for getSectionsAsJSON
        String query = "SELECT * FROM section WHERE termid = ? AND subjectid = ? AND num = ?";

        try {
            PreparedStatement prepared = connection.prepareStatement(query);
            prepared.setInt(1, termid);
            prepared.setString(2, subjectid);
            prepared.setString(3, num);
            ResultSet rst = prepared.executeQuery();
            result = getResultSetAsJSON(rst); 
        }
        
        catch (Exception e) { e.printStackTrace(); }
        return result;
        
    }
    
    public int register(int studentid, int termid, int crn) {
        
        int result = 0;
        
        // New query for register
        String query = "INSERT INTO registration (studentid, termid, crn) VALUES (?, ?, ?)";

        try {
            PreparedStatement prepared = connection.prepareStatement(query);
            prepared.setInt(1, studentid);
            prepared.setInt(2, termid);
            prepared.setInt(3, crn);
            result = prepared.executeUpdate();
        }
        
        catch (Exception e) { e.printStackTrace(); }
        return result;
        
    }

    public int drop(int studentid, int termid, int crn) {
        
        int result = 0;
        
        // New query for drop
        String query = "DELETE FROM registration WHERE studentid = ? AND termid = ? AND crn = ?";

        try {
            PreparedStatement prepared = connection.prepareStatement(query);

            prepared.setInt(1, studentid);
            prepared.setInt(2, termid);
            prepared.setInt(3, crn);
            result = prepared.executeUpdate();
        } 
        catch (Exception e) { e.printStackTrace(); }
        return result;
        
    }
    
    public int withdraw(int studentid, int termid) {
        
        int result = 0;
        
        // New query for withdraw
        String query = "DELETE FROM registration WHERE studentid = ? AND termid = ?";

        try {
            PreparedStatement prepared = connection.prepareStatement(query);

            prepared.setInt(1, studentid);
            prepared.setInt(2, termid);

            result = prepared.executeUpdate();
        } 
        catch (Exception e) { e.printStackTrace(); }
        return result;
        
    }
    
    public String getScheduleAsJSON(int studentid, int termid) {
        
        String result = null;
        
        // New query for getScheduleAsJSON
        String query = "SELECT * FROM registration r JOIN section s on s.crn = r.crn"+ " WHERE r.studentid = ? AND r.termid = ?";

        try {
            PreparedStatement prepared = connection.prepareStatement(query);

            prepared.setInt(1, studentid);
            prepared.setInt(2, termid);

            if(prepared.execute()){
                ResultSet resultset = prepared.getResultSet();
                result = getResultSetAsJSON(resultset); 
            }
        } 
        catch (Exception e) { e.printStackTrace(); }
        return result;
        
    }
    
    public int getStudentId(String username) {
        
        int id = 0;
        
        try {
        
            String query = "SELECT * FROM student WHERE username = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, username);
            
            boolean hasresults = pstmt.execute();
            
            if ( hasresults ) {
                
                ResultSet resultset = pstmt.getResultSet();
                
                if (resultset.next())
                    
                    id = resultset.getInt("id");
                
            }
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return id;
        
    }
    
    public boolean isConnected() {

        boolean result = false;
        
        try {
            
            if ( !(connection == null) )
                
                result = !(connection.isClosed());
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    /* PRIVATE METHODS */

    private Connection openConnection(String u, String p, String a) {
        
        Connection c = null;
        
        if (a.equals("") || u.equals("") || p.equals(""))
            
            System.err.println("*** ERROR: MUST SPECIFY ADDRESS/USERNAME/PASSWORD BEFORE OPENING DATABASE CONNECTION ***");
        
        else {
        
            try {

                String url = "jdbc:mysql://" + a + "/jsu_sp22_v1?autoReconnect=true&useSSL=false&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=America/Chicago";
                // System.err.println("Connecting to " + url + " ...");

                c = DriverManager.getConnection(url, u, p);

            }
            catch (Exception e) { e.printStackTrace(); }
        
        }
        
        return c;
        
    }
    
    private String getResultSetAsJSON(ResultSet resultset) {
        
        String result;
        
        /* Create JSON Containers */
        
        JSONArray json = new JSONArray();
        JSONArray keys = new JSONArray();
        
        try {
            
            /* Get Metadata */
        
            ResultSetMetaData metadata = resultset.getMetaData();
            int columnCount = metadata.getColumnCount();
            
            //Populating the JSONarray
            while(resultset.next()){
                JSONObject obj = new JSONObject();
                for (int i = 1; i <= columnCount; i++) {
                    String key = metadata.getColumnName(i);
                    String values = resultset.getString(i);

                    keys.add(key);
                    obj.put(key, values);
                }
                json.add(obj);
            }
        }
        catch (Exception e) { e.printStackTrace(); }
        
        /* Encode JSON Data and Return */
        
        result = JSONValue.toJSONString(json);
        return result;
        
    }
    
}