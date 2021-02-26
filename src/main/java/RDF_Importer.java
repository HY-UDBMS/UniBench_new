import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import java.util.Properties;

public class RDF_Importer {
    public static void main(String[] args) {
        /*
        Properties info = new Properties();
        info.put("user", "root");
        info.put("password", "123");
        Connection conn=null;
        try {
            conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:remote:localhost:2424/test", info);
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT firstName FROM Person limit 1");

        rs.next();
        System.out.println(rs.getString("firstName"));
        rs.close();
        stmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        ODatabaseDocumentTx db = new ODatabaseDocumentTx(
                "remote:localhost/test").open("root", "123");
        ODocument doc = new ODocument("Person");
        doc.field( "name", "Chao" );
        doc.field( "http//chaozhang", "helsinki" );
        doc.save();
        db.close();
*/
    // Import the RDF nodes

    // 1. read the data to JSON objects
    BufferedReader reader;
		try {
        reader = new BufferedReader(new FileReader(
                "RDF_product_nodes.json"));
        String line = reader.readLine();
        while (line != null) {
            //System.out.println(line);
            // read next line
            line = reader.readLine();
            if(line!=null){
                ObjectMapper mapper = new ObjectMapper();
                JsonNode actualObj = mapper.readTree(line);
                Iterator<String> itr = actualObj.fieldNames();
                while (itr.hasNext()) {  //to get the key fields
                    String key_field = itr.next();
                    String new_key=key_field.replaceAll("[:.]+", "");
                    System.out.println(new_key);
                }
                //String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(actualObj);
                // print json
                //System.out.println(json);
            }
        }
        reader.close();
    } catch (IOException e) {
        e.printStackTrace();
    }
    // 2. replace the string with DOT and Colon

    // 3. save the docs
    }
}
