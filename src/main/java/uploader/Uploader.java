package uploader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Uploader {

    private List<String> createCommand(String catalog, String newcatalog, String action) {
        List<String> command = new ArrayList<>();
        command.add("/usr/local/intermedium/bin/aws");
        command.add("s3");
        command.add(action);
        command.add("--follow-symlinks");
        if ("cp".equals(action)) {
            command.add("--recursive");
        }
        command.add("--include=\"*.pdf\"");
        command.add("--include=\"*.jpg\"");
        command.add("--no-progress");
        command.add("--profile");
        command.add("prod");
        command.add("/data/pdf/" + catalog);
        command.add("s3://mnd-mm-media-prod/paper/" + newcatalog);

        return command;
    }

    private void upload(String catalog, String newcatalog, String action) {
        try {
            ProcessBuilder builder = new ProcessBuilder();
            builder.command(createCommand(catalog, newcatalog, action));

            System.out.println(action + " " + catalog + " -> " + newcatalog);
            long startTime = System.currentTimeMillis();

            Process process = builder.start();
            int exitCode = process.waitFor();

            long used = System.currentTimeMillis() - startTime;
            System.out.println("finished " + catalog + " -> " + newcatalog + " (" + used + ")");

            if (exitCode != 0) {
                System.out.println("exitcode = " + exitCode + " for " + catalog + " -> " + newcatalog);
                System.out.println(output(process.getInputStream()));
                System.out.println(output(process.getErrorStream()));
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("ERROR when uploading " + catalog + " -> " + newcatalog);
            System.out.println(e.toString());
        }
    }


    private static String output(InputStream inputStream) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line + System.getProperty("line.separator"));
            }
        } finally {
            br.close();
        }
        return sb.toString();
    }


    public static void main(String[] args) {
        String file = null;

        if (args.length == 1) {
            file = args[0];
        } else {
            System.out.println("Synopsis:");
            System.out.println("java Uploader <file>");
            System.out.println("  prefix lines with *C for copy, *S for sync");
            System.exit(0);
        }

        Uploader uploader = new Uploader();
        try {
            List<String> allLines = Files.readAllLines(Paths.get(file));

            for (String line : allLines) {
                if (line.startsWith("*C ") || line.startsWith("*S ")) {
                    String action = line.startsWith("*C ") ? "cp" : "sync";
                    String[] parts = line.split(" ");
                    String catalog = null;
                    String newcatalog = null;
                    if (parts.length == 2) {
                        catalog = newcatalog = parts[1].trim();
                    } else if (parts.length == 3) {
                        catalog = parts[1].trim();
                        newcatalog = parts[2].trim();
                    } else {
                        System.out.println("ERROR skipping: " + line);
                    }
                    uploader.upload(catalog, newcatalog, action);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}