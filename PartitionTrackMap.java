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
@desc: Program saves the track to table name mapping in the table “icesat_partitions.track_tables” for better query performance.
@project_url: www.openaltimetry.org

*/


package org.openaltimetry.icesat.db.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class PartitionTrackMap {

    public static void getTracks(String table, Connection connection, Map<Integer, Set<String>> track2tables) throws Exception {

        Statement stmt = connection.createStatement();
        String sql = "SELECT DISTINCT track_id FROM " + table;
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            int trackId = rs.getInt(1);
            Set<String> tables = track2tables.get(trackId);
            if (tables == null) {
                tables = new TreeSet<String>();
            }
            tables.add(table);
            track2tables.put(trackId, tables);
        }
    }

    public static void save(Integer track, String table, Connection connection) throws Exception {
        
        Statement stmt = connection.createStatement();
        String sql = "INSERT INTO icesat_partitions.track_tables(track_id, table_name) VALUES("+track+", '"+table+"')";
        stmt.execute(sql);
        
    }
    
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/icesat", "USERNAME", "PASSWORD");
        Map<Integer, Set<String>> track2tables = new TreeMap<Integer, Set<String>>();
        try {
            for (int lat = -90; lat < 90; lat = lat + 1) {
                for (int lon = 0; lon < 360; lon = lon + 1) {
                    String tableName = "icesat_partitions.\"t_" + lat + "_" + lon+"\"";
                    getTracks(tableName, connection, track2tables);
                }
            }
            
            for (Integer track : track2tables.keySet()) {
                for (String table : track2tables.get(track)) {
                    save(track, table, connection);
                }
            }
            
        } finally {
            connection.close();
        }
    }

}
