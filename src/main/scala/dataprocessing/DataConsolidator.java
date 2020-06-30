package dataprocessing;

import java.io.*;
import java.nio.file.Paths;

/*
         Data Consolidation is the first step in this process. This process assumes a user ratings file for each movie
         is present in the specified input directory. Ex of a file looks like one provided below for movie 1
         Filename : mv_0000001.txt

         1:
          1488844,3,2005-09-06
          822109,5,2005-05-13
          885013,4,2005-10-19
          30878,4,2005-12-26
          823519,3,2004-05-03
          893988,3,2005-11-17
          124105,4,2004-08-05
          1248029,3,2004-04-22
          1842128,4,2004-05-09

          Data Consolidation process will run through each file and does the following operations
          1. Read the first line
          2. split by ":" to extract the moviedId
          3. append the movieId to the end of every line to make it ready for data analysis

          At the end of the process we will have one output file of all movies
           information and the data provided looks like as shown below.

          1488844,3,2005-09-06,1
          822109,5,2005-05-13,1
          885013,4,2005-10-19,1
          30878,4,2005-12-26,1
          823519,3,2004-05-03,1
          893988,3,2005-11-17,1
          124105,4,2004-08-05,1
          1248029,3,2004-04-22,1
          1842128,4,2004-05-09,1


     */

public class DataConsolidator {


    public static void fileMerge(String inputPath, String outputPath, String filename) throws IOException {
        /*
         Creates the instance of File by converting
         input path string into an abstract path value.

        */
        File filesInputDirectory = new File(inputPath);
        File outputFileDirectory = new File(outputPath);

        BufferedReader bufferedReader;
        BufferedWriter bufferedWriter = null;

        if (filesInputDirectory.exists() && filesInputDirectory.isDirectory()) {
            File[] arrayofFiles = filesInputDirectory.listFiles();
            System.out.println("Total no of files in the directory are " + arrayofFiles.length);


            if (outputFileDirectory.exists() && outputFileDirectory.isDirectory()) {
                bufferedWriter = new BufferedWriter(new FileWriter(new File(outputPath, filename)));
            } else {

                String currentDirectory = Paths.get(".").toAbsolutePath().normalize().toString();
                System.out.println(currentDirectory);
                bufferedWriter = new BufferedWriter(new FileWriter(new File(currentDirectory, filename)));
            }

            for (File file : arrayofFiles) {
                if (file.isFile()) {
                    bufferedReader = new BufferedReader(new FileReader(file));
                    String[] firstLine = bufferedReader.readLine().split(":");
                    String newLine = "";
                    while ((newLine = bufferedReader.readLine()) != null) {
                        if (newLine.length() > 7) { /* Assuming one character for each field
                                                   seperated by commas. Otherwise its considered
                                                    bad record */
                            newLine = newLine + "," + firstLine[0] + "\n";
                            bufferedWriter.write(newLine);
                        }


                    }
                    bufferedReader.close();
                }

            }
            bufferedWriter.close();

        } else {
            System.out.println("Please check if input path is a directory or not");
        }


    }


}
