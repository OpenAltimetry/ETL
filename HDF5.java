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
@desc: Program extracts and converts select data elements from a HDF5 file to CSV for loading into database.
The data elements include
	Data_40HZ_Time_d_UTCTime_40
	Data_40HZ_Geolocation_d_lat
	Data_40HZ_Geolocation_d_lon
	Data_40HZ_Elevation_Surfaces_d_elev
	Data_1HZ_Geolocation_i_track
Downloaded HDF5 file from NSIDC at ftp://n5eil01u.ecs.nsidc.org/SAN/GLAS/GLAH06.034	
@project_url: www.openaltimetry.org

*/

package jnative;

// Import hdf5 libraries
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.structs.H5G_info_t;

// Import opencsv libraries
import com.opencsv.CSVWriter;

// Import java libraries
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class HDF5 {

    // 1Hz data
    private static double[] data1_time;
    private static int[] data1_trackId;
    private static int[] data1_time_idx;

    // 40Hz data
    private static double[] data40_time;
    private static double[] data40_lat;
    private static double[] data40_lon;
    private static double[] data40_elev;
    private static int[] data40_time_idx;

    // Count total rows extracted for 40hz data
    private static long count_rows = 0;

    // Count total files accessed from 40hz data
    private static int count_files = 0;

    // A flag to tell which data (1hz or 40hz) that currently extracted
    private static boolean flag_1hz = false;

    // Formatter of timestamp for database
    private static final SimpleDateFormat DB_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    // Number of miliseconds from January 1, 1970, 00:00:00 GMT to January 1, 2000, 12:00:00 GMT
    // Should be: 946728000000
    private static final long J2000 = 946728000000L;

    @SuppressWarnings("ConvertToTryWithResources")
    public static void main(String args[]) throws Exception {
        // Always use UTC timezone for Icesat data, DO NOT use local timezone
        DB_FORMATTER.setTimeZone(TimeZone.getTimeZone("UTC"));

        //Calendar cal2000 = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        //cal2000.set(2000,0,1,12,0,0);
        //cal2000.set(Calendar.MILLISECOND, 0);
        //J2000 = cal2000.getTimeInMillis();
        // Get current start time
        Date startTime = new Date();
        System.out.println("Program started at: " + startTime.toString());

        try {
            // Data base directory 
            String data_dir = "\\SVN-OpenTopo\\Icesat_Data\\Icesat\\GLASH06.034";

            // Java base directory File object
            File data_dir_obj = new File(data_dir);

            // If not found Data base directory, return error
            if (!data_dir_obj.exists() || !data_dir_obj.isDirectory()) {
                System.err.println("Data directory not found");
                return;
            }

            // Prepare a log file in CSV format that list all H5 being process
            File csvOut_fileId = new File(data_dir_obj, "table_fileId.csv");

            // CSV writer for csvOut_fileId above
            CSVWriter csvWriter_fileId = new CSVWriter(new FileWriter(csvOut_fileId, false), ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER);

            // Increase file ID for database
            int filedb_Id = 0;

            // List of sub directories in data base directory
            String[] data_dir_list = data_dir_obj.list();

            // Scan each sub dir, directory name should be in YMD format: ex. 2003.02.20
            for (String ymd_dir : data_dir_list) {

                // File object for each sub dir
                File ymd_dir_obj = new File(data_dir_obj, ymd_dir);

                // Just make sure this is a directory
                if (ymd_dir_obj.isDirectory()) {
                    // Get the list of HDF5 files in each sub directory
                    String[] ymd_dir_list = ymd_dir_obj.list();

                    // Each data sub directory, generate one output CSV file for Data 40_Hz
                    File csvOut_40hz = new File(data_dir, "table_40hz_" + ymd_dir + ".csv");
                    System.out.println("csvOut 40hz: " + csvOut_40hz.getAbsolutePath());

                    // Open new CSV writer for Data 40_Hz
                    CSVWriter csvWriter_ymd_40hz = new CSVWriter(new FileWriter(csvOut_40hz, false), ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER);

                    // Each data sub directory, generate one output CSV file for Data 1_Hz
                    File csvOut_1hz = new File(data_dir, "table_1hz_" + ymd_dir + ".csv");
                    System.out.println("csvOut 1hz: " + csvOut_1hz.getAbsolutePath());

                    // Open new CSV writer for Data 1_Hz
                    CSVWriter csvWriter_ymd_1hz = new CSVWriter(new FileWriter(csvOut_1hz, false), ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER);

                    // Scan for each HDF5 file in directory 
                    for (String file_name : ymd_dir_list) {
                        // HDF5 file object
                        File hdf5_file_obj = new File(ymd_dir_obj, file_name);

                        // Make sure HDF5 file has extension .H5 
                        if (hdf5_file_obj.isFile() && file_name.endsWith(".H5")) {
                            // Increase count_files
                            count_files++;
                            System.out.print("\t" + count_files + " - " + file_name + "\t");

                            // Get absolute path for HDF5 file
                            String hdf5_name = hdf5_file_obj.getAbsolutePath();

                            /*
                             * Each Icesat HDF5 data has specific name convenion format:
                             * ex: GLAH06_634_1102_001_0072_1_01_0001.H5
                             * GLAxx_ymm_prkk_ccc_tttt_s_nn_ffff.eee 
                             * GLAH06_634_1102_001_0072_1_01_0001.H5
                             * 
                             * xx = Type ID number
                             * y = “Y” code that indicates calibration levels of POD and PAD.  Refer to the “YXX Release Number Convention Document” for details.
                             * mm = Release number for GSAS version that created the product.
                             * p = Repeat ground track phase
                             * r = Reference orbit number 
                             * kk = instance #incremented every time we enter a different reference orbit
                             * ccc = Cycle (000-999)
                             * tttt = Track (0000-2600)
                             * s = Segment, (0=none, 1-4 correspond to 50° lat/lon breaks)
                             * nn = granule version number (the number of times this granule is created for a specific release)
                             * ffff =file type. (numerical, CCB assigned for multiple files as needed for data of same time period for a specific HHHxx, i.e. multi-file granule)
                             * eee = file extension- dat for GLA01-15; qap for corresponding qap file;
                             * hdf for HDF browse packages; png for corresponding non-hdf browse files; vav for corresponding validation and verification file; met for metadata files
                             */
                            String[] fn_tokens = file_name.split("\\.");

                            String[] tokens = fn_tokens[0].split("_");
                            String token_xx = tokens[0];
                            String token_ymm = tokens[1];
                            String token_prkk = tokens[2];
                            String token_ccc = tokens[3];
                            String token_tttt = tokens[4];
                            String token_s = tokens[5];
                            String token_nn = tokens[6];
                            String token_ffff = tokens[7];
                            String token_eee = fn_tokens[1];

                            // First, open HDF5 file to get file_id
                            int file_id = H5.H5Fopen(hdf5_name, HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);

                            //System.out.println("file_id: " + file_id);
                            // Validate HDF5 by making sure file_id > 0
                            if (file_id >= 0) {
                                // Increase filedb_Id for database table
                                filedb_Id++;

                                // Reset all data containers before process new file
                                data40_time = null;
                                data40_lat = null;
                                data40_lon = null;
                                data40_elev = null;
                                data1_time = null;
                                data1_trackId = null;

                                // Write CSV File metadata that extracted from file name above
                                csvWriter_fileId.writeNext(new String[]{
                                    String.valueOf(filedb_Id),
                                    ymd_dir_obj.getPath(),
                                    file_name,
                                    token_xx,
                                    token_ymm,
                                    token_prkk,
                                    token_ccc,
                                    token_tttt,
                                    token_s,
                                    token_nn,
                                    token_ffff,
                                    token_eee
                                });

                                // Start extracting data by getting group_id
                                int group_id = H5.H5Gopen(file_id, "/", HDF5Constants.H5P_DEFAULT);

                                // Validate by making sure group_id >= 0
                                if (group_id >= 0) {
                                    // Recursively call this function to extract data through HDF5
                                    printGroup(group_id, "/", "");

                                    // Done extracting data, if data 40hz contaner is not empty then start writing data to CSV
                                    if (data40_time != null && data40_time.length > 0) {

                                        // For loop to access each point from data 40hz
                                        for (int m = 0; m < data40_time.length; m++) {

                                            // Ignore records that have NULL values for LAT
                                            // Which are marked with max float number 
                                            if (data40_lat[m] > 90) {
                                                continue;
                                            }

                                            // Ignore records that have NULL values for LNG
                                            // Which are marked with max float number 
                                            if (data40_lon[m] > 360) {
                                                continue;
                                            }

                                            // Good data, write each record to CSV for data 40hz
                                            csvWriter_ymd_40hz.writeNext(new String[]{
                                                String.valueOf(filedb_Id),
                                                String.valueOf(data40_time[m]),
                                                convertTimestamp(data40_time[m]),
                                                String.valueOf(data40_lat[m]),
                                                String.valueOf(data40_lon[m]),
                                                String.valueOf(data40_elev[m]),
                                                String.valueOf(data40_time_idx[m])
                                            });
                                        }
                                    }

                                    // Done extracting data, if data 1hz contaner is not empty then start writing data to CSV
                                    if (data1_time != null && data1_time.length > 0) {

                                        // For loop to access each point from data 1hz
                                        // Remember for data 1hz we don't need to validate data as data 40hz
                                        // because data 1hz is for mapping track index with 40hz only
                                        for (int m = 0; m < data1_time.length; m++) {

                                            // Write each record to CSV for data 1hz
                                            csvWriter_ymd_1hz.writeNext(new String[]{
                                                String.valueOf(filedb_Id),
                                                String.valueOf(data1_time[m]),
                                                convertTimestamp(data1_time[m]),
                                                String.valueOf(data1_trackId[m]),
                                                String.valueOf(data1_time_idx[m])
                                            });
                                        }
                                    }

                                    // Close the group.
                                    H5.H5Gclose(group_id);
                                }

                                // Close the file.
                                H5.H5Fclose(file_id);

                            }
                        }
                    }

                    // Close the CSV writers.
                    csvWriter_ymd_40hz.close();
                    csvWriter_ymd_1hz.close();
                }
            }

            // Close the CSV writers.
            csvWriter_fileId.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Get current end time to tell the runtime
        Date endTime = new Date();
        System.out.println("Program started at: " + startTime.toString());
        System.out.println("Program endded at: " + endTime.toString());

        System.out.println("Total runtime: " + getTimeDifference(endTime, startTime));
    }

    /**
     * Recursively print a group and its members.
     *
     * @throws Exception
     */
    private static void printGroup(int g_id, String gname, String indent) throws Exception {
        //System.out.println("gname: " + gname);
        if (g_id < 0) {
            return;
        }

        try {
            H5G_info_t members = H5.H5Gget_info(g_id);
            String objNames[] = new String[(int) members.nlinks];
            int objTypes[] = new int[(int) members.nlinks];
            int lnkTypes[] = new int[(int) members.nlinks];
            long objRefs[] = new long[(int) members.nlinks];
            int names_found = 0;

            try {
                names_found = H5.H5Gget_obj_info_all(
                        g_id,
                        null,
                        objNames,
                        objTypes,
                        lnkTypes,
                        objRefs,
                        HDF5Constants.H5_INDEX_NAME
                );
            } catch (Throwable err) {
                //err.printStackTrace();
            }

            indent += "\t";

            for (int i = 0; i < names_found; i++) {
                switch (objNames[i]) {
                    // Getting data1_trackId 
                    case "i_track": {
                        int dataset_id = H5.H5Dopen(g_id, objNames[i], HDF5Constants.H5P_DEFAULT);
                        long npoints = H5.H5Sget_select_npoints(H5.H5Dget_space(dataset_id));
                        data1_trackId = new int[(int) npoints];
                        H5.H5Dread(dataset_id,
                                HDF5Constants.H5T_NATIVE_INT,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5P_DEFAULT,
                                data1_trackId
                        );
                        H5.H5Dclose(dataset_id);
                        break;
                    }
                    // Getting UTC Time value for data 40hz 
                    case "d_UTCTime_40": {
                        int dataset_id = H5.H5Dopen(g_id, objNames[i], HDF5Constants.H5P_DEFAULT);
                        long npoints = H5.H5Sget_select_npoints(H5.H5Dget_space(dataset_id));
                        count_rows += npoints;
                        System.out.println("\tTotal count: " + count_rows);

                        data40_time = new double[(int) npoints];
                        H5.H5Dread(dataset_id,
                                HDF5Constants.H5T_NATIVE_DOUBLE,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5P_DEFAULT,
                                data40_time
                        );
                        H5.H5Dclose(dataset_id);

                        flag_1hz = true;
                        break;
                    }
                    // Getting UTC Time indexing value for data 40hz and data 1hz, 
                    // Both data 1hz and data 40hz using these values to compare and map
                    // to get the track_id from data 1hz that data 40hz don't have
                    case "i_rec_ndx": {
                        int dataset_id = H5.H5Dopen(g_id, objNames[i], HDF5Constants.H5P_DEFAULT);
                        long npoints = H5.H5Sget_select_npoints(H5.H5Dget_space(dataset_id));

                        // Using this flag to tell this is data 1hz or data 40hz
                        // This is data 40hz
                        if (flag_1hz) {
                            data40_time_idx = new int[(int) npoints];
                            H5.H5Dread(dataset_id,
                                    HDF5Constants.H5T_NATIVE_INT,
                                    HDF5Constants.H5S_ALL,
                                    HDF5Constants.H5S_ALL,
                                    HDF5Constants.H5P_DEFAULT,
                                    data40_time_idx
                            );
                            H5.H5Dclose(dataset_id);

                            //System.out.println("\tTotal count data40_time_idx: " + data40_time_idx.length);                        
                        } // This is data 1hz
                        else {
                            data1_time_idx = new int[(int) npoints];
                            H5.H5Dread(dataset_id,
                                    HDF5Constants.H5T_NATIVE_INT,
                                    HDF5Constants.H5S_ALL,
                                    HDF5Constants.H5S_ALL,
                                    HDF5Constants.H5P_DEFAULT,
                                    data1_time_idx
                            );
                            H5.H5Dclose(dataset_id);

                            //System.out.println("\tTotal count data1_time_idx: " + data1_time_idx.length);                        
                        }

                        break;
                    }
                    // Getting UTC Time for data 1hz, 
                    case "d_UTCTime_1": {
                        int dataset_id = H5.H5Dopen(g_id, objNames[i], HDF5Constants.H5P_DEFAULT);
                        long npoints = H5.H5Sget_select_npoints(H5.H5Dget_space(dataset_id));

                        data1_time = new double[(int) npoints];
                        H5.H5Dread(dataset_id,
                                HDF5Constants.H5T_NATIVE_DOUBLE,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5P_DEFAULT,
                                data1_time
                        );
                        H5.H5Dclose(dataset_id);

                        flag_1hz = false;
                        break;
                    }

                    // Getting latitude for data 40hz, 
                    case "d_lat": {
                        int dataset_id = H5.H5Dopen(g_id, objNames[i], HDF5Constants.H5P_DEFAULT);
                        long npoints = H5.H5Sget_select_npoints(H5.H5Dget_space(dataset_id));
                        data40_lat = new double[(int) npoints];
                        H5.H5Dread(dataset_id,
                                HDF5Constants.H5T_NATIVE_DOUBLE,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5P_DEFAULT,
                                data40_lat
                        );
                        H5.H5Dclose(dataset_id);
                        break;
                    }
                    // Getting longitude for data 40hz, 
                    case "d_lon": {
                        int dataset_id = H5.H5Dopen(g_id, objNames[i], HDF5Constants.H5P_DEFAULT);
                        long npoints = H5.H5Sget_select_npoints(H5.H5Dget_space(dataset_id));
                        data40_lon = new double[(int) npoints];
                        H5.H5Dread(dataset_id,
                                HDF5Constants.H5T_NATIVE_DOUBLE,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5P_DEFAULT,
                                data40_lon
                        );
                        H5.H5Dclose(dataset_id);
                        break;
                    }
                    // Getting elevation for data 40hz, 
                    case "d_elev": {
                        int dataset_id = H5.H5Dopen(g_id, objNames[i], HDF5Constants.H5P_DEFAULT);
                        long npoints = H5.H5Sget_select_npoints(H5.H5Dget_space(dataset_id));
                        data40_elev = new double[(int) npoints];
                        H5.H5Dread(dataset_id,
                                HDF5Constants.H5T_NATIVE_DOUBLE,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5S_ALL,
                                HDF5Constants.H5P_DEFAULT,
                                data40_elev
                        );
                        H5.H5Dclose(dataset_id);
                        break;
                    }

                    default:
                        break;
                }

                // Recursive call to traverse through the file
                if (objTypes[i] == HDF5Constants.H5O_TYPE_GROUP) {
                    // Open the group, obtaining a new handle.

                    int group_id = H5.H5Gopen(g_id, objNames[i], HDF5Constants.H5P_DEFAULT);

                    if (group_id >= 0) {
                        printGroup(group_id, objNames[i], indent);

                        // Close the group. 
                        H5.H5Gclose(group_id);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } catch (Throwable t) {
            t.printStackTrace();
        }

    }

    /**
     * This function to convert data40_time or data1_time to timestamp in UTC
     * timezone
     */
    public static String convertTimestamp(double d) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        long time_ms = (long) (d * 1000);
        cal.setTimeInMillis(J2000 + time_ms);
        return DB_FORMATTER.format(cal.getTime());
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
