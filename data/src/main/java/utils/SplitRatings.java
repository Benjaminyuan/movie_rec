package utils;

import java.io.*;
import java.util.HashMap;

public class SplitRatings {
    private static String template = "/Users/mac/Downloads/moivelens/ml-latest/split/ratings-%d.csv";
    private static int sizePerFile = 100;

    public static void spiltData() throws IOException {
        File f = new File("/Users/mac/Downloads/moivelens/ml-latest/ratings.csv");
        FileInputStream inputStream = new FileInputStream(f);
        InputStreamReader fileInputStream = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(fileInputStream);
        reader.readLine();
        String line = null;
        long nextUserId = sizePerFile;
        long curUserId = 0;

        BufferedWriter curWriter = createNextWriter(0);
        while ((line = reader.readLine()) != null) {
            String[] list = line.split(",");
            if (list.length >= 4) {
                curUserId = Long.parseLong(list[0]);
                if (nextUserId < curUserId) {
                    curWriter.flush();
                    curWriter.close();
                    curWriter = createNextWriter(nextUserId / 100);
                    nextUserId += sizePerFile;

                }
                System.out.println("userId: " + curUserId);
                curWriter.write(line + "\n");

            }
        }
    }

    public static BufferedWriter createNextWriter(long id) throws IOException {
        String path = String.format(template, id);
        File f = new File(path);
        if(!f.exists() && !f.createNewFile()){
            System.out.println("fail to create file");
        }
        FileOutputStream inputStream = new FileOutputStream(f);
        OutputStreamWriter fileInputStream = new OutputStreamWriter(inputStream);
        return new BufferedWriter(fileInputStream);
    }
}
