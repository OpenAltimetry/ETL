/*
Copyright (c) 2007 The Regents of the University of California

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research and non-profit purposes, without fee, and without a written
agreement is hereby granted, provided that the above copyright notice, this
paragraph and the following three paragraphs appear in all copies.

Permission to make commercial use of this software may be obtained
by contacting:
Technology Transfer Office
9500 Gilman Drive, Mail Code 0910
University of California
La Jolla, CA 92093-0910
(858) 534-5815
invent@ucsd.edu

THIS SOFTWARE IS PROVIDED BY THE REGENTS OF THE UNIVERSITY OF CALIFORNIA AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT
NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

@author: Minh Phan (mnphan@ucsd.edu)
@date: July 24th 2017
@desc:  Program loads the ICESat data file in CSV format (generated from HDF5.java) into the postgres database table.
@project_url: www.openaltimetry.org
*/

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class LoadIcesatDB {

    // Postgresql database connection
    static public final String PDB_CONNECTION = Property.getProperty("pdb.connection");
    static public final String PDB_USERNAME = Property.getProperty("pdb.username");
    static public final String PDB_PASSWORD = Property.getProperty("pdb.password");
    static public Connection postgresConn = null;

    // Data dir that store CSV files to be loaded to database
    static public final String DATA_DIR = Property.getProperty("data_dir");

    // Variable to count table_40hz
    static public long count_table_40hz = 0;

    // Variable to count table_1hz
    static public long count_table_1hz = 0;

    // Variable to count number of row of table_fileId
    static public long count_table_fileId = 0;

    // Variable to count number of file_40Hz
    static public long count_file_40hz = 0;

    // Variable to count number of file_1Hz
    static public long count_file_1hz = 0;
    
    @SuppressWarnings("ConvertToTryWithResources")
    public static void main(String[] args) {
        Date startTime = new Date();
        System.out.println("Program started at: " + startTime.toString());
        
        try {
            // Connect to Postgresql database            
            if (!postgresConnect()) {
                System.out.println("Cannot connect to postgreSQL database");
                return;
            }
            System.out.println("Connected to postgreSQL database");

            String sql;
            FileReader reader;

            try {
                // Start loading process            
                CopyManager cm = new CopyManager((BaseConnection) postgresConn);

                // Get data dir object                
                File data_dir_obj = new File(DATA_DIR);

                if (!data_dir_obj.exists() || !data_dir_obj.isDirectory()) {
                    System.out.println("Data dir does not exist: " + DATA_DIR);
                    return;
                }

                // Load table_fileId.csv to table, this is just metadata table
                sql = "COPY HDF5_FILE FROM STDIN WITH DELIMITER AS ',' CSV";
                reader = new FileReader(new File(data_dir_obj, "table_fileId.csv"));
                count_table_fileId = cm.copyIn(sql, reader);
                reader.close();

                // Get all csv files as File object
                File[] csv_file_objs = data_dir_obj.listFiles();

                // Scan for each CSV file
                for (File csv_file_obj : csv_file_objs) {
                    // Make sure it is a file
                    if (csv_file_obj.isDirectory()) {
                        continue;
                    }

                    // Make sure it is a CSV extension
                    String fileName = csv_file_obj.getName();
                    if (!fileName.endsWith(".csv")) {
                        continue;
                    }

                    // If it is a CSV file for table_1hz
                    if (fileName.startsWith("table_1hz")) {
                        count_file_1hz++;

                        sql = "COPY DATA_1HZ (FILEID, EPOCH_2000_1, DATETIME_1, TRACK_ID, TIME_IDX) FROM STDIN WITH DELIMITER AS ',' CSV";
                        reader = new FileReader(csv_file_obj);
                        count_table_1hz += cm.copyIn(sql, reader);
                        reader.close();
                        System.out.println(count_file_1hz + "\n" + fileName);                        
                    } // Else if it is a CSV file for table_40hz                    
                    else if (fileName.startsWith("table_40hz")) {
                        count_file_40hz++;

                        sql = "COPY DATA_40HZ (FILEID, EPOCH_2000_40, DATETIME_40, LAT, LON, ELEV, TIME_IDX) FROM STDIN WITH DELIMITER AS ',' CSV";
                        reader = new FileReader(csv_file_obj);
                        count_table_40hz += cm.copyIn(sql, reader);
                        reader.close();
                        System.out.println(count_file_40hz + "\n" + fileName);                        
                    }
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                // Don't forget to disconnect database before exit
                postgresDisconnect();
                System.out.println("\nDisconnected to postgreSQL database");
            }

            System.out.println("count_file_40hz: " + count_file_40hz);
            System.out.println("count_file_1hz: " + count_file_1hz);
            System.out.println("count_table_40hz: " + count_table_40hz);
            System.out.println("count_table_1hz: " + count_table_1hz);
            System.out.println("count_table_fileId: " + count_table_fileId);

            /*
            count_file_40hz: 597
            count_file_1hz: 597
            count_table_40hz: 1038759677
            count_table_1hz: 36336251
            count_table_fileId: 34208
             */

        } catch (Exception e) {
            e.printStackTrace();
        }

        Date endTime = new Date();
        System.out.println("Program started at: " + startTime.toString());
        System.out.println("Program endded at: " + endTime.toString());

        System.out.println("Total runtime: " + getTimeDifference(endTime, startTime));
                
    }

    public static boolean postgresConnect() {
        return postgresConnect(false);
    }

    public static boolean postgresConnect(boolean readonly) {

        try {
            postgresConn = DriverManager.getConnection(PDB_CONNECTION, PDB_USERNAME, PDB_PASSWORD);
            postgresConn.setReadOnly(readonly);
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Desc: Close connection from Postgres Database
     *
     * @return
     */
    public static boolean postgresDisconnect() {
        try {
            if (postgresConn != null) {
                postgresConn.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    public static int executeUpdateQuery(String sql) {
        System.out.println(sql);
        int rows = -1;
        try {
            Statement stmt = postgresConn.createStatement();
            rows = stmt.executeUpdate(sql);
            stmt.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        System.out.println("Returned: " + rows + "\n------------------");
        return rows;
    }
    
    /**
     * @param d2 the later date
     * @param d1 the earlier date
     * @return
     */
    public static String getTimeDifference(Date d2, Date d1) {
        Date diff = new Date(d2.getTime() - d1.getTime());

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTime(diff);
        int hours = calendar.get(Calendar.HOUR_OF_DAY);
        int minutes = calendar.get(Calendar.MINUTE);
        int seconds = calendar.get(Calendar.SECOND);

        return hours + "h:" + minutes + "m:" + seconds + "s";
    }    
}
