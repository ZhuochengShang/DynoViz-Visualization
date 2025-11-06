package edu.ucr.cs.bdlab.dynoviz;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.WebMethod;
import edu.ucr.cs.bdlab.beast.util.AbstractWebHandler;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.davinci.DaVinciServer;
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.DownloadDataInVisibleRegions;
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.GetPointValue;
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.TileGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


//TODO Handle outputs for all methods
public class RaptorServer extends AbstractWebHandler {

    Long timeOnthefly = 0L;
    Long imgOnthefly = 0L;
    private static final Log LOG = LogFactory.getLog(DaVinciServer.class);

    @OperationParam(
            description = "Enable/Disable server-side caching of generated tiles",
            defaultValue = "true"
    )
    public static final String ServerCache = "cache";

    @OperationParam(
            description = "Path to the directory that contains the tiles and data",
            defaultValue = "."
    )
    public static final String DataDirectory = "datadir";

    /**The file system that contains the data dir*/
    protected FileSystem dataFileSystem;

    /**The Spark context of the underlying application*/
    protected JavaSparkContext sc;

    /**
     * A server-side cache that caches copies of generated tiles to avoid regenerating the same tile for multiple users.
     */
    private Cache<String,byte[]> cache;

    /**Path to the data directory*/
    protected Path datadir;

    private BeastOptions beastOpts;

    @Override
    public void setup(SparkContext sc, BeastOptions opts) {
        super.setup(sc, opts);
        this.sc = new JavaSparkContext(sc);
        this.beastOpts = opts;
        if (opts.getBoolean(ServerCache, true)) {
            cache = CacheBuilder.newBuilder()
                    .maximumSize(100000) // the cache size is 100,000 tiles
                    .expireAfterAccess(30, TimeUnit.DAYS)
                    .build();
        }
        this.datadir = new Path(opts.getString(DataDirectory, "."));
        try {
            this.dataFileSystem = this.datadir.getFileSystem(opts.loadIntoHadoopConf(sc.hadoopConfiguration()));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Cannot retrieve the file system of the data directory '%s'", this.datadir), e);
        }
    }


    /**
     * Handles the response request to fetch the value for given point and dataset.
     * @param target the target path requested by the user
     * @param request the HTTP request
     * @param response the HTTP response
     * @param datasetID the path to the dataset
     * @param x the X coordinate of the point
     * @param y the Y coordinate of the point
     * @throws IOException if an error happens while handling the request
     * @return {@code true if the request was handled}
     */
    @WebMethod(url = "/dynamic/raptor.cgi/pointvalue/{datasetID}/{x}/{y}")
    public boolean getPointValue(String target,
                                 HttpServletRequest request,
                                 HttpServletResponse response,
                                 String datasetID,
                                 String x, String y) throws IOException {
        String[] inputs = {datasetID, x, y};

        String responseValue = GetPointValue.run(beastOpts,inputs, sc.sc());

        // Set the response status and content type
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json");

        // Write the response as JSON
        JsonGenerator jsonGenerator = new JsonFactory().createGenerator(response.getOutputStream());
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("value", responseValue);
        jsonGenerator.writeEndObject();
        jsonGenerator.close();

        return true;
    }

    /**
     * Handles the request to generate and stream the subset of the dataset as a zip file.
     *
     * @param target        the target path requested by the user
     * @param request       the HTTP request
     * @param response      the HTTP response
     * @param datasetName   the name of the dataset
     * @param topLeftX     the top-left X coordinate
     * @param topLeftY    the top-left Y coordinate
     * @param bottomRightX   the bottom-right X coordinate
     * @param bottomRightY   the bottom-right Y coordinate
     * @return {@code true} if the request was handled successfully; {@code false} otherwise
     * @throws IOException if an error occurs during zip file generation or streaming
     */
    @WebMethod(url = "/dynamic/raptor.cgi/dowloadsubset/{datasetName}/{topLeftX}/{topLeftY}/{bottomRightX}/{bottomRightY}")
    public boolean handleSubsetDownload(String target,
                                        HttpServletRequest request,
                                        HttpServletResponse response,
                                        String datasetName,
                                        String topLeftX,
                                        String topLeftY,
                                        String bottomRightX,
                                        String bottomRightY) throws IOException {

        // Generate default zip file based on datasetName, topLeft, and bottomRight
        String[] inputs = {topLeftX, topLeftY, bottomRightX, bottomRightY, datasetName};

        String filesToBeZippedStr = DownloadDataInVisibleRegions.run(inputs, sc.sc());
        String[] filePaths = filesToBeZippedStr.split("\\|\\|");
        if(filePaths.length<1)
        {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json");
            // Write the response as JSON
            JsonGenerator jsonGenerator = new JsonFactory().createGenerator(response.getOutputStream());
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("Error", "No data available in the selected region");
            jsonGenerator.writeEndObject();
            jsonGenerator.close();
            return true;
        }

        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=archive.zip");
        FileSystem fs = FileSystem.get(URI.create(inputs[4]),sc.hadoopConfiguration());

        try (ZipOutputStream zipOutputStream = new ZipOutputStream(response.getOutputStream())) {
            for (String filePath : filePaths) {
                Path hdfsPath = new Path(filePath);
                FSDataInputStream inputStream = fs.open(hdfsPath);
                ZipEntry entry = new ZipEntry(hdfsPath.getName());
                zipOutputStream.putNextEntry(entry);

                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    zipOutputStream.write(buffer, 0, bytesRead);
                }
                zipOutputStream.closeEntry();
            }
        } catch (Exception e) {
            // Handle exceptions (log or send an error response)
            e.printStackTrace();
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return false;
        }
        return true;
    }

    /**
     * Handles the response request to fetch the tile as PNG for given ZXY coordinates.
     *
     * @param target     the target path requested by the user
     * @param request    the HTTP request
     * @param response   the HTTP response
     * @param datasetID  the path to the dataset
     * @param workingDir the working directory path
     * @param z          the zoom level
     * @param x          the X coordinate of the tile
     * @param y          the Y coordinate of the tile
     * @throws IOException if an error happens while handling the request
     * @return {@code true} if the request was handled
     */
    @WebMethod(url = "{datasetID}/tile/{workingDir}/tile-{z}-{x}-{y}.png")
    public boolean getTile(String target,
                           HttpServletRequest request,
                           HttpServletResponse response,
                           String datasetID,
                           String workingDir,
                           String z,
                           String x, String y) throws IOException, URISyntaxException {
        long startTime = System.nanoTime();
        StringBuilder howHandled = new StringBuilder();

        String filename = String.format("tile-%s-%s-%s.png", z, x, y);
        workingDir = "file://"+workingDir;
        URI workingDirURI =  new URI(workingDir);
        Path tilePath = new Path(workingDir, filename);

        FileSystem fs = FileSystem.get(tilePath.toUri(), new Configuration());
        byte[] tileData;

        if (fs.exists(tilePath) && fs.getFileStatus(tilePath).isFile()) {
            howHandled.append("static-file");

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("image/png");
            response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");

            try (InputStream inputStream = fs.open(tilePath);
                 OutputStream outputStream = response.getOutputStream()) {
                byte[] buffer = new byte[2048];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }
        } else if (cache != null && (tileData = cache.getIfPresent(tilePath.toString())) != null) {
            howHandled.append("server-cached");
            OutputStream finalOutput = response.getOutputStream();
            finalOutput.write(tileData);
            finalOutput.close();
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("image/png");
            response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        } else {
            howHandled.append("on-the-fly");

            String[] inputs = {"file://"+datasetID, z, x, y};

            BufferedImage bufferedImage = TileGenerator.run(inputs, sc.sc());

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("image/png");
            response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");

            try (OutputStream outputStream = response.getOutputStream()) {
                ImageIO.write(bufferedImage, "png", outputStream);
                if(cache!=null){
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    outputStream.write(byteArrayOutputStream.toByteArray());
                    cache.put(tilePath.toString(), byteArrayOutputStream.toByteArray());
                }
            }

            // Save the generated tile to working directory
            try {
                FileSystem outFS = tilePath.getFileSystem(new Configuration());
                LOG.debug("Attempting to write to: " + tilePath + " in " + outFS.toString());
                FSDataOutputStream outputStream = outFS.create(tilePath);
                ImageIO.write(bufferedImage, "png", outputStream);
                outputStream.close();
            } catch (Exception e) {
                LOG.error("Exception: " + e.getMessage());
                throw new IOException("Error saving tile to HDFS", e);
            }
            timeOnthefly += ( (System.nanoTime() - startTime) );
            imgOnthefly += 1;
            LOG.info(String.format("Until Current, Requested generated total img '%s' processed in %f seconds (%s)",
                    imgOnthefly, timeOnthefly * 1E-9, howHandled));
        }
        long endTime = System.nanoTime();
        //LOG.info(String.format("Requested tile '%s' processed in %f seconds (%s)",
        //        filename, (endTime - startTime) * 1E-9, howHandled));
        return true;
    }

}
