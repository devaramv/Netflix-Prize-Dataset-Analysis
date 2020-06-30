package properties;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GetConfigurations {

    private static Logger logger = Logger.getLogger(GetConfigurations.class);

    private static InputStream stream;
    private static String individual_MovieRatings_Path;
    private static String consolidated_MovieRatings_Path;
    private static String movie_titles_location;
    private static String consolidated_Output_FileName;
    private static String final_Output_Location;
    private static int R;
    private static int M;
    private static int U;

    public static void loadProperties(String configFilePath) throws IOException {

        try{
            Properties properties = new Properties();
            stream = new FileInputStream(new File(configFilePath));
            properties.load(stream);

            individual_MovieRatings_Path = properties.getProperty("individual_MovieRatings_Path");
            consolidated_MovieRatings_Path = properties.getProperty("consolidated_MovieRatings_Path");
            consolidated_Output_FileName=properties.getProperty("consolidated_Output_FileName");
            final_Output_Location = properties.getProperty("final_Output_Location");
            movie_titles_location = properties.getProperty("movie_titles_Location");
            R = Integer.parseInt(properties.getProperty("R"));
            M = Integer.parseInt(properties.getProperty("M"));
            U = Integer.parseInt(properties.getProperty("U"));
        }catch (IOException e){
            logger.info("Exception encountered while reading input file" + e);
        }




    }

    public static String getIndividual_MovieRatings_Path() {
        return individual_MovieRatings_Path;
    }


    public static String getConsolidated_MovieRatings_Path() {
        return consolidated_MovieRatings_Path;
    }


    public static String getMovie_titles_location() {
        return movie_titles_location;
    }


    public static int getR() {
        return R;
    }


    public static int getM() {
        return M;
    }


    public static int getU() {
        return U;
    }

    public static String getConsolidated_Output_FileName() {
        return consolidated_Output_FileName;
    }

    public static String getFinal_Output_Location() {
        return final_Output_Location;
    }


}
