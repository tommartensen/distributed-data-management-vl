package de.hpi.akka_assignment.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataLoader {

    public static Map<String, List<Object>> loadTable(String filePath) {

        String line = "";
        String cvsSplitBy = ";";

        Map<String, List<Object>> students = new HashMap<>();
        List<Object> ids = new ArrayList<>();
        List<Object> names = new ArrayList<>();
        List<Object> passwordHashes = new ArrayList<>();
        List<Object> genes = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] content = line.split(cvsSplitBy);

                ids.add(Integer.valueOf(content[0]));
                names.add(content[1]);
                passwordHashes.add(content[2]);
                genes.add(content[3]);
            }
            students.put("ids", ids);
            students.put("names", names);
            students.put("passwordHashes", passwordHashes);
            students.put("genes", genes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return students;
    }
}
