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

@author: Kai Lin (klin@ucsd.edu)
@date: July 24th 2017
@desc: Program to partition the larger ICESat table “data_40hz” into smaller tables in the schema “icesat_partition” for better query performance. Each partitioned table contains all the points in one latitude degree by one longitude degree box.
@project_url: www.openaltimetry.org

*/

package org.openaltimetry.icesat.db.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class ICESATDBPartition {

    private static void createTableAndInsert(Connection connection, int lat, int lng) throws Exception {

        Statement stmt = connection.createStatement();

        // drop the table if exists
        String sql = "DROP TABLE IF EXISTS icesat_partitions.\"t_" + lat + "_" + lng + "\" CASCADE";
        stmt.execute(sql);
        
        // create a table for the lat/lng
        sql = "CREATE TABLE icesat_partitions.\"t_" + lat + "_" + lng + "\" (\n"
                + "    id integer  PRIMARY KEY,\n"
                + "    datetime_40 timestamp without time zone,\n"
                + "    lat double precision,\n"
                + "    lon double precision,\n"
                + "    elev double precision,\n"
                + "    track_id integer\n"
                + ");          ";
        stmt.execute(sql);

        // insert data into the table
        sql = "INSERT INTO icesat_partitions.\"t_" + lat + "_" + lng + "\" \n"
                + "SELECT id, datetime_40, lat, lon, elev, track_id \n"
                + "FROM data_40hz \n"
                + "WHERE (lat > " + lat + " and lat < " + (lat + 1) + ") \n"
                + "AND (lon > " + lng + " and lon < " + (lng + 1) + ")";
        stmt.execute(sql);
        
        // create index on lat/lng/time
        sql = "CREATE INDEX \"t_" + lat + "_" + lng + "_lat_lon_time_idx\" "
                + "ON icesat_partitions.\"t_" + lat + "_" + lng + "\"(lat, lon, datetime_40)";
        stmt.execute(sql);

    }

    public static void main(String[] args) throws Exception {

        Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/icesat", "USERNAME", "PASSWORD");

        try {
            for (int lat = -90; lat < 90; lat = lat + 1) {
                for (int lon = 0; lon < 360; lon = lon + 1) {
                    System.out.println("-------------------------");
                    System.out.println("lat="+lat+", lon="+lon);
                    createTableAndInsert(connection, lat, lon);
                }
            }
        } finally {
            connection.close();
        }
    }

}
