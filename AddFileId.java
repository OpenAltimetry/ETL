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
@desc: Program adds two new columns “file_id” and “link” to each partition table and populates the columns. (Original NSIDC HDF5 link back)
@project_url: www.openaltimetry.org

*/


package org.openaltimetry.icesat.db.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class AddFileId {
    
    private static void doit(Connection connection, int lat, int lng) throws Exception {

        Statement stmt = connection.createStatement();

        // add the column fileid
        String sql = "ALTER TABLE icesat_partitions.\"t_" + lat + "_" + lng + "\" ADD COLUMN fileid integer";
	try {
            stmt.execute(sql);
	} catch (Exception ex) {
	    System.out.println(sql);
	    System.out.println(ex.getMessage());
	}

        // insert data into the fileld
        sql = "UPDATE icesat_partitions.\"t_" + lat + "_" + lng + "\" \n"
                + "SET fileid = data_40hz.fileid \n"
                + "FROM data_40hz \n"
                + "WHERE icesat_partitions.\"t_" + lat + "_" + lng + "\".id = data_40hz.id ";

	try {
	    stmt.execute(sql);
	} catch (Exception ex) {
	    System.out.println(sql);
	    System.out.println(ex.getMessage());
	}

    }


    private static void doLink(Connection connection, int lat, int lng) throws Exception {

        Statement stmt = connection.createStatement();

        // add the column fileid
        String sql = "ALTER TABLE icesat_partitions.\"t_" + lat + "_" + lng + "\" ADD COLUMN link varchar(256)";
	try {
            stmt.execute(sql);
	} catch (Exception ex) {
	    System.out.println(sql);
	    System.out.println(ex.getMessage());
	}

        // insert data into the fileld
        sql = "UPDATE icesat_partitions.\"t_" + lat + "_" + lng + "\" \n"
                + "SET link = hdf5_file.link \n"
                + "FROM hdf5_file \n"
                + "WHERE icesat_partitions.\"t_" + lat + "_" + lng + "\".fileid = hdf5_file.fileid ";

	try {
	    stmt.execute(sql);
	} catch (Exception ex) {
	    System.out.println(sql);
	    System.out.println(ex.getMessage());
	}

    }
    

    public static void main(String[] args) throws Exception {

        Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/icesat", "USERNAME", "PASSWORD");

        try {
            for (int lat = -90; lat < 90; lat = lat + 1) {
                for (int lon = 0; lon < 360; lon = lon + 1) {
                    System.out.println("-------------------------");
                    System.out.println("lat="+lat+", lon="+lon);
                    doit(connection, lat, lon);
                    doLink(connection, lat, lon);
                }
            }
        } finally {
            connection.close();
        }
    }

}
