import java.sql.Connection;
import java.sql.DriverManager;

public class TestOracle {
    public static void main(String[] args) {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            System.out.println("Driver loaded!");
            Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@oracle-db:1521/FREE", "c##dbzuser", "dbz");
            System.out.println("Connected!");
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
