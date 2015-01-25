/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitter_sample;


import java.sql.Connection;     
import java.sql.DatabaseMetaData;     
import java.sql.Driver;     
import java.sql.DriverManager;     
import java.sql.ResultSet;     
import java.sql.SQLException;     
import java.sql.Statement;     
import java.util.Enumeration;     
import java.util.Vector;     
    
public class ConnectionPool {     
    
    private String jdbcDriver = ""; //      
    
    private String dbUrl = ""; //  URL     
    
    private String dbUsername = ""; //     
    
    private String dbPassword = ""; //      
    
    private String testTable = ""; // 
    private int initialConnections = 1; // 
    
    private int incrementalConnections = 1;// 
    
    private int maxConnections = 100; // 
    
    private Vector connections = null; //     
    
    public ConnectionPool() {            
    }     
         
    /**   
     *    
     * ������������   
     *    
     * @param jdbcDriver   
     *            String JDBC ������������   
     * @param dbUrl   
     *            String ��������� URL   
     * @param dbUsername   
     *            String ������������������������   
     * @param dbPassword   
     *            String ������������������������������   
     */    
    
    public ConnectionPool(String jdbcDriver, String dbUrl, String dbUsername,     
            String dbPassword) {     
    
        this.jdbcDriver = jdbcDriver;     
    
        this.dbUrl = dbUrl;     
    
        this.dbUsername = dbUsername;     
    
        this.dbPassword = dbPassword;     
    
    }     
    
    /**   
     * ������������������������������������������������������������������������������������ initialConnections ���������������   
     */    
    
    public synchronized void createPool() throws Exception {     
    
        // ���������������������������     
    
        // ������������������������������������������������������ connections ������������     
    
        if (connections != null) {     
    
            return; // ������������������������������     
    
        }     
    
        // ��������� JDBC Driver ���������������������������     
    
        Driver driver = (Driver) (Class.forName(this.jdbcDriver).newInstance());     
    
        DriverManager.registerDriver(driver); // ������ JDBC ������������     
    
        // ��������������������������� , ������������ 0 ���������     
    
        connections = new Vector();     
    
        // ������ initialConnections ���������������������������������     
    
        createConnections(this.initialConnections);     
    
        System.out.println(" ..................");     
    
    }     
    
    /**   
     *    
     * ��������� numConnections ������������������������������ , ������������������   
     *    
     * ������ connections ���������   
     *    
     *    
     *    
     * @param numConnections   
     *            ������������������������������������   
     *    
     */    
    
    @SuppressWarnings("unchecked")     
    private void createConnections(int numConnections) throws SQLException {     
    
        // ������������������������������������������     
    
        for (int x = 0; x < numConnections; x++) {     
    
            // ��������������������������������������������������������������������������������������� maxConnections     
    
            // ��������������� maxConnections ��� 0 ���������������������������������������������     
    
            // ������������������������������������������������     
    
            if (this.maxConnections > 0    
                    && this.connections.size() >= this.maxConnections) {     
    
                break;     
    
            }     
    
            // add a new PooledConnection object to connections vector     
    
            // ������������������������������������������ connections ������     
    
            try {     
    
                connections.addElement(new ConnectionPool.PooledConnection(newConnection()));     
    
            } catch (SQLException e) {     
    
                System.out.println(" ............... " + e.getMessage());     
    
                throw new SQLException();     
    
            }     
    
            System.out.println("  ......");     
    
        }     
    
    }     
    
    /**   
     *    
     * ���������������������������������������������   
     *    
     *    
     *    
     * @return ���������������������������������������   
     *    
     */    
    
    private Connection newConnection() throws SQLException {     
    
        // ���������������������������     
    
        Connection conn = DriverManager.getConnection(dbUrl, dbUsername,     
                dbPassword);     
    
        // ���������������������������������������������������������������������������������������������������     
    
        // ������������������������     
    
        // connections.size()==0 ������������������������������������     
    
        if (connections.size() == 0) {     
    
            DatabaseMetaData metaData = conn.getMetaData();     
    
            int driverMaxConnections = metaData.getMaxConnections();     
    
            // ������������������ driverMaxConnections ������ 0 ���������������������������������     
    
            // ���������������������������������������������������������     
    
            // driverMaxConnections ������������������������������������������������������������������������     
    
            // ��������������������������������������������������������������������������������� , ������������������������     
    
            // ���������������������������������������������     
    
            if (driverMaxConnections > 0    
                    && this.maxConnections > driverMaxConnections) {     
    
                this.maxConnections = driverMaxConnections;     
    
            }     
    
        }     
    
        return conn; // ������������������������������������     
    
    }     
    
    /**   
     *    
     * ������������ getFreeConnection() ������������������������������������������ ,   
     *    
     * ������������������������������������������������������������������������������������������������������������������������������������������������������������������   
     *    
     * @return ������������������������������������������   
     *    
     */    
    
    public synchronized Connection getConnection() throws SQLException {     
    
        // ���������������������������     
    
        if (connections == null) {     
    
            return null; // ��������������������������������� null     
    
        }     
    
        Connection conn = getFreeConnection(); // ������������������������������������     
        
        // ���������������������������������������������������������������������������     
    
        while (conn == null) {     
    
            // ���������������     
            System.out.println("ConnectionPool: no free connection now!");
            wait(250);     
    
            conn = getFreeConnection(); // ���������������������������������������������������     
    
            // getFreeConnection() ������������ null     
    
            // ���������������������������������������������������������     
    
        }     
    
        return conn;// ������������������������������     
    
    }     
    
    /**   
     *    
     * ��������������������������� connections ���������������������������������������������������   
     *    
     * ��������������������������������������������������������� incrementalConnections ������   
     *    
     * ������������������������������������������������������������   
     *    
     * ��������������������������������������������������������������� null   
     *    
     * @return ������������������������������������   
     *    
     */    
    
    private Connection getFreeConnection() throws SQLException {     
    
        // ���������������������������������������������������     
    
        Connection conn = findFreeConnection();     
    
        if (conn == null) {     
    
            // ���������������������������������������������     
    
            // ������������������     
    
            createConnections(incrementalConnections);     
    
            // ������������������������������������������     
    
            conn = findFreeConnection();     
    
            if (conn == null) {     
    
                // ��������������������������������������������������������������� null     
    
                return null;     
    
            }     
    
        }     
    
        return conn;     
    
    }     
    
    /**   
     *    
     * ���������������������������������������������������������������������������   
     *    
     * ������������������������������������ null   
     *    
     *    
     *    
     * @return ������������������������������������   
     *    
     */    
    
    private Connection findFreeConnection() throws SQLException {     
    
        Connection conn = null;     
    
        ConnectionPool.PooledConnection pConn = null;     
    
        // ���������������������������������������     
    
        Enumeration enumerate = connections.elements();     
    
        // ���������������������������������������������������     
    
        while (enumerate.hasMoreElements()) {     
    
            pConn = (ConnectionPool.PooledConnection) enumerate.nextElement();     
    
            if (!pConn.isBusy()) {     
    
                // ������������������������������������������������������������������������     
    
                conn = pConn.getConnection();     
    
                pConn.setBusy(true);     
    
                // ���������������������������     
    
                if (!testConnection(conn)) {     
    
                    // ���������������������������������������������������������������     
    
                    // ������������������������������������������������������������������ null     
    
                    try {     
    
                        conn = newConnection();     
    
                    } catch (SQLException e) {     
    
                        System.out.println(" .............. " + e.getMessage());     
    
                        return null;     
    
                    }     
    
                    pConn.setConnection(conn);     
    
                }     
    
                break; // ������������������������������������������     
    
            }     
    
        }     
    
        return conn;// ������������������������������     
    
    }     
    
    /**   
     * ��������������������������������������������������������������������� false ������������������ true   
     *    
     * @param conn   
     *            ������������������������������   
     * @return ������ true ������������������������ false ���������������   
     */    
    
    private boolean testConnection(Connection conn) {     
    
        try {     
    
            // ���������������������������     
    
            if (testTable.equals("")) {     
    
                // ������������������������������������������������ setAutoCommit() ������     
    
                // ��������������������������������������������������������������������������������� ,     
    
                // ������������������������������������������������������������     
    
                conn.setAutoCommit(true);     
    
            } else {// ������������������������������������������     
    
                // check if this connection is valid     
    
                Statement stmt = conn.createStatement();     
    
                ResultSet rs = stmt.executeQuery("select count(*) from "    
                        + testTable);     
    
                rs.next();     
    
                System.out.println(testTable + "..............." + rs.getInt(1));     
    
            }     
    
        } catch (SQLException e) {     
    
            // ������������������������������������������������������������������ false;     
            e.printStackTrace();     
                 
            closeConnection(conn);     
    
            return false;     
    
        }     
    
        // ��������������������� true     
    
        return true;     
    
    }     
    
    /**   
     * ������������������������������������������������������������������������������������   
     *    
     * ���������������������������������������������������������������������������������������   
     *    
     * @param ���������������������������������������   
     */    
    
    public void returnConnection(Connection conn) {     
    
        // ������������������������������������������������������������������������������     
    
        if (connections == null) {     
    
            System.out.println("...........!");     
    
            return;     
    
        }     
    
        ConnectionPool.PooledConnection pConn = null;     
    
        Enumeration enumerate = connections.elements();     
    
        // ������������������������������������������������������������������������     
    
        while (enumerate.hasMoreElements()) {     
    
            pConn = (ConnectionPool.PooledConnection) enumerate.nextElement();     
    
            // ������������������������������������������������     
    
            if (conn == pConn.getConnection()) {     
    
                // ��������� , ������������������������������     
    
                pConn.setBusy(false);     
    
                break;     
    
            }     
    
        }     
    
    }     
    
    /**   
     * ���������������������������������������   
     */    
    
    public synchronized void refreshConnections() throws SQLException {     
    
        // ������������������������������     
    
        if (connections == null) {     
    
            System.out.println(" .................. !");     
    
            return;     
    
        }     
    
        ConnectionPool.PooledConnection pConn = null;     
    
        Enumeration enumerate = connections.elements();     
    
        while (enumerate.hasMoreElements()) {     
    
            // ������������������������     
    
            pConn = (ConnectionPool.PooledConnection) enumerate.nextElement();     
    
            // ��������������������� 5 ��� ,5 ������������������     
    
            if (pConn.isBusy()) {     
    
                wait(5000); // ��� 5 ���     
    
            }     
    
            // ���������������������������������������������������     
    
            closeConnection(pConn.getConnection());     
    
            pConn.setConnection(newConnection());     
    
            pConn.setBusy(false);     
    
        }     
    
    }     
    
    /**   
     * ���������������������������������������������������������   
     */    
    
    public synchronized void closeConnectionPool() throws SQLException {     
    
        // ������������������������������������������������     
    
        if (connections == null) {     
    
            System.out.println("..... !");     
    
            return;     
    
        }     
    
        ConnectionPool.PooledConnection pConn = null;     
    
        Enumeration enumerate = connections.elements();     
    
        while (enumerate.hasMoreElements()) {     
    
            pConn = (ConnectionPool.PooledConnection) enumerate.nextElement();     
    
            // ��������������� 5 ���     
    
            if (pConn.isBusy()) {     
    
                wait(5000); // ��� 5 ���     
    
            }     
    
            // 5 ���������������������     
    
            closeConnection(pConn.getConnection());     
    
            // ������������������������������     
    
            connections.removeElement(pConn);     
    
        }     
    
        // ������������������     
    
        connections = null;     
    
    }     
    
    /**   
     * ���������������������������   
     *    
     * @param ������������������������������   
     */    
    
    private void closeConnection(Connection conn) {     
    
        try {     
    
            conn.close();     
    
        } catch (SQLException e) {     
    
            System.out.println(" ................ " + e.getMessage());     
    
        }     
    
    }     
    
    /**   
     * ���������������������������������   
     *    
     * @param ������������������   
     */    
    
    private void wait(int mSeconds) {     
    
        try {     
    
            Thread.sleep(mSeconds);     
    
        } catch (InterruptedException e) {     
    
        }     
    
    }     
    
    /**   
     * ������������������������������   
     *    
     * @return ������������������������������������������   
     */    
    
    public int getInitialConnections() {     
    
        return this.initialConnections;     
    
    }     
    
    /**   
     * ������������������������������   
     *    
     * @param ���������������������������������������������   
     */    
    
    public void setInitialConnections(int initialConnections) {     
    
        this.initialConnections = initialConnections;     
    
    }     
    
    /**   
     * ������������������������������������ ���   
     *    
     * @return ������������������������������   
     */    
    
    public int getIncrementalConnections() {     
    
        return this.incrementalConnections;     
    
    }     
    
    /**   
     * ������������������������������������   
     *    
     * @param ������������������������������   
     */    
    
    public void setIncrementalConnections(int incrementalConnections) {     
    
        this.incrementalConnections = incrementalConnections;     
    
    }     
    
    /**   
     * ���������������������������������������������   
     *    
     * @return ���������������������������������������   
     */    
    
    public int getMaxConnections() {     
    
        return this.maxConnections;     
    
    }     
    
    /**   
     * ���������������������������������������������   
     *    
     * @param ������������������������������������������������   
     */    
    
    public void setMaxConnections(int maxConnections) {     
    
        this.maxConnections = maxConnections;     
    
    }     
    
    /**   
     * ���������������������������������   
     *    
     * @return ���������������������������   
     */    
    
    public String getTestTable() {     
    
        return this.testTable;     
    
    }     
    
    /**   
     * ������������������������   
     *    
     * @param testTable   
     *            String ������������������   
     */    
    
    public void setTestTable(String testTable) {     
    
        this.testTable = testTable;     
    
    }     
    
    /**   
     * ���������������������������������������������������������   
     *    
     * ������������������������������������������������������������������������������������������   
     *    
     * ������������������������   
     */    
    
    class PooledConnection {     
    
        Connection connection = null;// ���������������     
    
        boolean busy = false; // ���������������������������������������������������������������     
    
        // ��������������������������� Connection ������������ PooledConnection ������     
    
        public PooledConnection(Connection connection) {     
    
            this.connection = connection;     
    
        }     
    
        // ���������������������������     
    
        public Connection getConnection() {     
    
            return connection;     
    
        }     
    
        // ���������������������������     
    
        public void setConnection(Connection connection) {     
    
            this.connection = connection;     
    
        }     
    
        // ���������������������������     
    
        public boolean isBusy() {     
    
            return busy;     
    
        }     
    
        // ������������������������������     
    
        public void setBusy(boolean busy) {     
    
            this.busy = busy;     
    
        }     
    
    }     
    
}  

