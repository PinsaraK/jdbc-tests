import java.util.*;
import java.util.Date;
import java.net.*;
import java.text.*;
import java.lang.*;
import java.io.*;
import java.sql.*;
import pgpass.*;

public class AddPurchase {

    private HashMap<String, String> options;
    private Connection connection;
    private String url;
    private String user;

    private Integer custID;     
    private String custName;
    private String clubName;
    private String bookTitle;
    private Integer bookYear;
    

    public AddPurchase(HashMap<String, String> options){

        try{
            Class.forName("org.postgresql.Driver");
        } catch(Exception e){
            System.out.println(e);
        }

        this.options = options;
        if (options.containsKey("-u")){
            user = options.get("-u");
        }
        //url = "jdbc:postgresql://db:5432/<dbname>?currentSchema=yrb";
        url = "jdbc:postgresql://db:5432/" ;

        Properties props = new Properties();

        try {
            String passwd = PgPass.get("db", "*", user, user);
            props.setProperty("user", user);
            //System.out.println(passwd);
            props.setProperty("password", passwd);
            // props.setProperty("ssl","true"); // NOT SUPPORTED on DB
        } catch(PgPassException e) {
            System.out.print("\nCould not obtain PASSWD from <.pgpass>.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        try {
            // Connect with a fall-thru id & password
            //conDB = DriverManager.getConnection(url,"<username>","<password>");
            connection = DriverManager.getConnection(url, props);
        } catch(SQLException e) {
            System.out.print("\nSQL: database connection error.\n");
            System.out.println(e.toString());
            System.exit(0);
        } 

        try {
            connection.setAutoCommit(false);
        } catch(SQLException e) {
            System.out.print("\nFailed trying to turn autocommit off.\n");
            e.printStackTrace();
            System.exit(0);
        }  

        if(!customerCheck()){
            System.out.print("There is no customer #");
            System.out.print(custID);
            System.out.println(" in the database.");
            System.out.println("Bye.");
            System.exit(0);
        }
        if(!clubCheck()){
            System.out.print("There is no club ");
            System.out.print(clubName);
            System.out.println(" in the database.");
            System.out.println("Bye.");
            System.exit(0);
        }
        if(!bookCheck()){
            System.out.print("There is no book ");
            System.out.print(bookTitle + ", " + bookYear);
            System.out.println(" in the database.");
            System.out.println("Bye.");
            System.exit(0);
        }
        if(!memberCheck()){
            System.out.print("There is no customer with id:" + custID + " in this club: ");
            System.out.print(clubName);
            System.out.println(" in the database.");
            System.out.println("Bye.");
            System.exit(0);
        }
        if(!offerCheck()){
            System.out.print("There is no offer for book: " + bookTitle + ", " + bookYear + " in this club: ");
            System.out.print(clubName);
            System.out.println(" in the database.");
            System.out.println("Bye.");
            System.exit(0);
        }

        Date today = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        //System.out.println(formatter.format(today));
        if (!options.get("-w").startsWith(formatter.format(today))){
            System.out.print("Purchase was not made today on ");
            System.out.print(formatter.format(today));
            System.out.println(". Bye.");
            System.exit(0);
        }

        if(Integer.parseInt(options.get("-q")) <= 0) {
            System.out.print("Invalid quantity. Quantity must be greater than 0.");
            System.out.println(" Bye.");
            System.exit(0);
        }

        //update purchase records
        insertRecord();


        try {
            connection.commit();
        } catch(SQLException e) {
            System.out.print("\nFailed trying to commit.\n");
            e.printStackTrace();
            System.exit(0);
        }    
        // Close the connection.
        try {
            connection.close();
        } catch(SQLException e) {
            System.out.print("\nFailed trying to close the connection.\n");
            e.printStackTrace();
            System.exit(0);
        }  
    }

    public void insertRecord() {
        String insertText = "";     // The SQL text.
        PreparedStatement insertSt = null;   // The query handle.
        //ResultSet myRs = null;   // A cursor.

        Integer cid;
        String club;
        String title;
        Integer year;
        Timestamp whenp = null;
        Integer qnty;

        insertText = " INSERT INTO yrb_purchase (cid, club, title, year, whenp, qnty) " +
                    "VALUES (?, ?, ?, ?, ?, ?) ";
        try {
            insertSt = connection.prepareStatement(insertText);
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }
        //insertSt = connection.prepareStatement(insertText);

        cid = Integer.parseInt(options.get("-c"));
        club = options.get("-b");
        title = options.get("-t");
        year = Integer.parseInt(options.get("-y"));
        
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date parsedDate = dateFormat.parse(options.get("-w"));
            whenp = new java.sql.Timestamp(parsedDate.getTime());
        } catch(Exception e) { //this generic but you can control another types of exception
            // look the origin of excption
            System.out.println("Exception in converting whenp string to timestamp");
            System.out.println(e.toString());
            System.exit(0);
        }
        qnty = Integer.parseInt(options.get("-q"));

        
        try {
            insertSt.setInt(1, cid);
            insertSt.setString(2, club);
            insertSt.setString(3, title);
            insertSt.setInt(4, year);
            insertSt.setTimestamp(5, whenp);
            insertSt.setInt(6, qnty);
            //System.out.println(insertSt.toString());
            insertSt.executeUpdate();
            System.out.println("data inserted into yrb_purchase");
        } catch(SQLException e) {
            System.out.println("INSERT failed in execute");
            System.out.println(e.toString());
            System.exit(0);
        }

        try {
            insertSt.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
        //myRs = insertSt.executeQuery();
        
    }

    public boolean offerCheck() {
        String queryText = "";     // The SQL text.
        PreparedStatement querySt = null;   // The query handle.
        ResultSet answers = null;   // A cursor.

        boolean inDB = false;  // Return.
        String club;
        String title;
        Integer year;

        queryText =
            "SELECT club, title, year "
          + "FROM yrb_offer "
          + "WHERE club = ? AND title = ? AND year = ? ";

        // Prepare the query.
        try {
            querySt = connection.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Execute the query.
        if (options.containsKey("-t") && options.containsKey("-b") && options.containsKey("-y")) {
            title = options.get("-t");
            year = Integer.parseInt(options.get("-y"));
            club = options.get("-b");
            try {
                querySt.setString(1, club);
                querySt.setString(2, title);
                querySt.setInt(3, year);
                answers = querySt.executeQuery();
            } catch(SQLException e) {
                System.out.println("SQL#1 failed in execute");
                System.out.println(e.toString());
                System.exit(0);
            }
        } else {
            return false;
        }
        
        // Any answer?
        try {
            if (answers.next()) {
                inDB = true;
                //custName = answers.getString("name");
            } else {
                inDB = false;
               // custName = null;
            }
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in cursor.");
            System.out.println(e.toString());
            System.exit(0);
        }
        
        // Close the cursor.
        try {
            answers.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing cursor.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        // We're done with the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
       // System.out.println("club: " + club + ", title: " + title + ", year: " + year);
        return inDB;
    }
    
    public boolean memberCheck() {
        String queryText = "";     // The SQL text.
        PreparedStatement querySt = null;   // The query handle.
        ResultSet answers = null;   // A cursor.

        boolean inDB = false;  // Return.
        String club;
        Integer cid;

        queryText =
            "SELECT club, cid "
          + "FROM yrb_member "
          + "WHERE club = ? AND cid = ? ";

        // Prepare the query.
        try {
            querySt = connection.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Execute the query.
        if (options.containsKey("-c") && options.containsKey("-b")) {
            cid = Integer.parseInt(options.get("-c"));
            club = options.get("-b");
            try {
                querySt.setString(1, club);
                querySt.setInt(2, cid);
                answers = querySt.executeQuery();
            } catch(SQLException e) {
                System.out.println("SQL#1 failed in execute");
                System.out.println(e.toString());
                System.exit(0);
            }
        } else {
            return false;
        }
        
        // Any answer?
        try {
            if (answers.next()) {
                inDB = true;
                //custName = answers.getString("name");
            } else {
                inDB = false;
               // custName = null;
            }
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in cursor.");
            System.out.println(e.toString());
            System.exit(0);
        }
        
        // Close the cursor.
        try {
            answers.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing cursor.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        // We're done with the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
        //System.out.println("club: " + club + ", cid: " + cid);
        return inDB;
    }

    public boolean customerCheck() {
        String queryText = "";     // The SQL text.
        PreparedStatement querySt = null;   // The query handle.
        ResultSet answers = null;   // A cursor.

        boolean inDB = false;  // Return.

        queryText =
            "SELECT name "
          + "FROM yrb_customer "
          + "WHERE cid = ? ";

        // Prepare the query.
        try {
            querySt = connection.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Execute the query.
        if (options.containsKey("-c")) {
            custID = Integer.parseInt(options.get("-c"));
            try {
                querySt.setInt(1, custID);
                answers = querySt.executeQuery();
            } catch(SQLException e) {
                System.out.println("SQL#1 failed in execute");
                System.out.println(e.toString());
                System.exit(0);
            }
        } else {
            return false;
        }
        
        // Any answer?
        try {
            if (answers.next()) {
                inDB = true;
                custName = answers.getString("name");
            } else {
                inDB = false;
                custName = null;
            }
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in cursor.");
            System.out.println(e.toString());
            System.exit(0);
        }
        
        // Close the cursor.
        try {
            answers.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing cursor.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        // We're done with the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        return inDB;
    }

    public boolean bookCheck() {
        String queryText = "";     // The SQL text.
        PreparedStatement querySt = null;   // The query handle.
        ResultSet answers = null;   // A cursor.

        boolean inDB = false;  // Return.
        String title = "";
        Integer year;

        queryText =
            "SELECT title, year "
          + "FROM yrb_book "
          + "WHERE title = ? AND year = ?";

        // Prepare the query.
        try {
            querySt = connection.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Execute the query.
        if (options.containsKey("-t") && options.containsKey("-y")) {
            year = Integer.parseInt(options.get("-y"));
            title = options.get("-t");
            try {
                querySt.setString(1, title);
                querySt.setInt(2, year);
                answers = querySt.executeQuery();
            } catch(SQLException e) {
                System.out.println("SQL#1 failed in execute");
                System.out.println(e.toString());
                System.exit(0);
            }
        } else{
            return false;
        }
        
        // Any answer?
        try {
            if (answers.next()) {
                inDB = true;
                bookTitle = answers.getString("title");
                bookYear = answers.getInt("year");
            } else {
                inDB = false;
                bookTitle = null;
                bookYear = null;
            }
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in cursor.");
            System.out.println(e.toString());
            System.exit(0);
        }
        
        // Close the cursor.
        try {
            answers.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing cursor.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        // We're done with the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
        //System.out.println("book title: " + bookTitle + ", bookyear: " + bookYear);
        return inDB;
    }

    public boolean clubCheck() {
        String queryText = "";     // The SQL text.
        PreparedStatement querySt = null;   // The query handle.
        ResultSet answers = null;   // A cursor.

        boolean inDB = false;  // Return.
        String club = "";

        queryText =
            "SELECT club "
          + "FROM yrb_club "
          + "WHERE club = ? ";

        // Prepare the query.
        try {
            querySt = connection.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Execute the query.
        if (options.containsKey("-b")) {
            club = options.get("-b");
            try {
                querySt.setString(1, club);
                answers = querySt.executeQuery();
            } catch(SQLException e) {
                System.out.println("SQL#1 failed in execute");
                System.out.println(e.toString());
                System.exit(0);
            }
        } else{
            return false;
        }
        
        // Any answer?
        try {
            if (answers.next()) {
                inDB = true;
                clubName = answers.getString("club");
            } else {
                inDB = false;
                clubName = null;
            }
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in cursor.");
            System.out.println(e.toString());
            System.exit(0);
        }
        
        // Close the cursor.
        try {
            answers.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing cursor.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        // We're done with the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
        return inDB;
    }
    public static void main(String[] args) {

        HashMap<String, String> options = new HashMap<String, String>();

        int i = 0;
        while (i < args.length){
            if (args[i].startsWith("-")) {
                if (i + 1 >= args.length || args[i + 1].startsWith("-")){
                    System.out.println("Incorrect usage.");
                    System.out.println("Usage: java AddPurchase -c <cid> -b <club> -t <title> -y <year> [-w <when>] [-q <qnty>] [-u <user>]");
                    System.exit(0);
                } else {
                    options.put(args[i], args[i + 1]);
                }
            }
            i++;
        }
        if (!(options.containsKey("-c") && options.containsKey("-b") && options.containsKey("-t") && options.containsKey("-y"))){
            System.out.println("Missing arguments");
            System.out.println("Usage: java AddPurchase -c <cid> -b <club> -t <title> -y <year> [-w <when>] [-q <qnty>] [-u <user>]");
            System.exit(0);
        }
        
        if (!options.containsKey("-w")){
            Date date = new Date();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            options.put("-w", formatter.format(date));
        }

        if (options.get("-w").length() < 12) {
            options.replace("-w", options.get("-w") + " 00:00:00");
        }

        if (!options.containsKey("-q")){
            options.put("-q", "1");
        }

        if(!options.containsKey("-u")){
            options.put("-u", "pinsara");
        }

        //System.out.println(options);
        AddPurchase ap = new AddPurchase(options);
        
       
    }
}