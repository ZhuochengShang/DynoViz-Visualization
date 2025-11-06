package edu.ucr.cs.bdlab.dynoviz;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class FetchImage {
    private static final Log LOG = LogFactory.getLog(FetchImage.class);

    public static void writeALL(String filePath) throws IOException {
        Long fileCount = 0L;
        Path path = Paths.get(filePath);
        try (Stream<Path> stream = Files.list(path)) {
            Stream<Path> streamF = stream.filter(f -> f.getFileName().toString().endsWith(".png"));
            String[] pathStrings = streamF
                    .map(Path::toString)
                    .toArray(String[]::new);
            Long StartTime = System.nanoTime();
            for(int i = 0; i<pathStrings.length; i++){
                String f = pathStrings[i];
                try {
                    FileInputStream fileInputStream = new FileInputStream(f);
                    BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    // Define a buffer size you consider appropriate
                    // Define a buffer size you consider appropriate
                    byte[] buffer = new byte[1024]; // Buffer size of 1024 bytes
                    int bytesRead = 0;

                    // Read the file contents into the buffer and process
                    while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
                        // Convert bytes to string and print
                        outputStream.write(buffer, 0, bytesRead);
                    }
                    outputStream.close();
                    fileInputStream.close();
                    fileCount +=1;
                    LOG.info(" Total time count: "+ (System.nanoTime()-StartTime)/1E9 );
                    LOG.info(" Total file count: "+ fileCount);
                } catch (FileNotFoundException ex) {
                    throw new RuntimeException(ex);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
