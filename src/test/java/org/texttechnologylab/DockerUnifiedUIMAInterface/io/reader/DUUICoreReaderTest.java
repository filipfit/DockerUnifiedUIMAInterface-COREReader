package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FloatArray;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.XmlCasSerializer;
import org.dkpro.core.io.xmi.XmiReader;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.CoreReader.CorePageUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.CoreReader.DUUICoreReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.CoreReader.ImageReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.CoreReader.TSVTable;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.annotation.FourPointBoundingBox;
import org.texttechnologylab.annotation.ImageBase64;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

class DUUICoreReaderTest {

    public static CollectionReaderDescription inputDirCas(String dir) throws ResourceInitializationException {
        return CollectionReaderFactory.createReaderDescription(
                XmiReader.class,
                XmiReader.PARAM_ADD_DOCUMENT_METADATA, false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA, false,
                XmiReader.PARAM_LENIENT, true,
                XmiReader.PARAM_SOURCE_LOCATION, dir);
    }

    @Test
    public void testHello() {
        System.out.println("Hello World!");
    }

    @Test
    public void testCreateTable() throws CsvValidationException, IOException {
        TSVTable myTable = new TSVTable("./testTables/screenshots.tsv");
        TSVTable newTable = myTable.getRows(0, 4);

        myTable.print();
        System.out.println();
        newTable.print();
        System.out.println();
        newTable.getTableMap();
        newTable.getTableMap();
    }

    @Test
    public void testExtractByValue() throws Exception {
        TSVTable t = new TSVTable("./testTables/screenshots.tsv");
        t = t.extractByValue("page_id", "24111");
        t.print();
    }

    @Test
    public void testToJcas() throws Exception {
        DUUICoreReader reader = new DUUICoreReader();

        JCas jcas = JCasFactory.createJCas();
        reader.getNextCas(jcas);

        // Create annotated output xmi file
        File xmiFile = new File("TEMP_out/24111.xmi");
        try(FileOutputStream out = new FileOutputStream(xmiFile)) {
            XmlCasSerializer.serialize(jcas.getCas(), out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("XMI file created: " + xmiFile.getAbsolutePath());
        System.out.println("24111");
    }

    @Test
    public void testHasNext() throws Exception {
        DUUICoreReader reader = new DUUICoreReader();
        JCas jcas = JCasFactory.createJCas();
        System.out.println("testHasNext temporaryPageIds.size: " + reader.pageIDs.size());

        System.out.println("testHasNext: " + reader.hasNext() + " atIndex: " + reader.nextIndex);
        reader.getNextCas(jcas);
        System.out.println("testHasNext: " + reader.hasNext() + " atIndex: " + reader.nextIndex);
        reader.getNextCas(jcas);
        System.out.println("testHasNext: " + reader.hasNext() + " atIndex: " + reader.nextIndex);
        reader.getNextCas(jcas);
        System.out.println("testHasNext: " + reader.hasNext() + " atIndex: " + reader.nextIndex);
        reader.getNextCas(jcas);
    }

    @Test
    public void testGetAllPageIDs() throws Exception {
        List<String> pageIDs = new TSVTable("TEMP_files/testTables/pages.tsv").getColumn(0);
        System.out.println(pageIDs);
    }

    @Test
    public void testMapUsersToPages() throws Exception {
        TSVTable pages = new TSVTable("TEMP_files/testTables/pages.tsv");
        TSVTable sessions = new TSVTable("TEMP_files/testTables/sessions.tsv");
        List<String> pageIDs = pages.getColumn(0);
        List<List<String>> pageSessionUser = new ArrayList<>();

        for (var id : pageIDs) {
            String sessionID = pages.extractByValue("id", id).getCell("session_id", 0);
            String userID = sessions.extractByValue("id", sessionID).getCell("user_id", 0);
            List<String> row = new ArrayList<>();
            row.add(id);
            row.add(sessionID);
            row.add(userID);
            pageSessionUser.add(row);
        }
    }

    @Test
    public void testMapPagesSessionsUsers() throws Exception{
        DUUICoreReader reader = new DUUICoreReader();
        List<List<String>> result = reader.getMappedPagesSessionsUsers();
        System.out.println(result);
    }

    @Test
    public void testGetPageID() throws Exception {
        DUUICoreReader reader = new DUUICoreReader();
        String session = reader.getSessionID("24111");
        System.out.println("pageID: 24111   sessionID: " + session);
    }

    @Test
    public void testGetUserID() throws Exception {
        DUUICoreReader reader = new DUUICoreReader();
        String user = reader.getUserID("24111");
        System.out.println(user);
    }

    @Test
    void testGetHTML() throws Exception {
        DUUICoreReader.getHtmlFileIDs("TEMP_files/TEMP_out/24111.xmi");
    }

    @Test
    void testFileSearch() throws Exception {
        TSVTable screenshotsTable = new TSVTable("TEMP_files/testTables/screenshots.tsv");
        TSVTable pageScreenshotData = screenshotsTable.extractByValue("page_id", "24111");
        Map<String, List<String>> tableMap = pageScreenshotData.getTableMap();

        Set<String> filesNames = Set.copyOf(tableMap.get("id"));
        Map<String, Path> filePaths =  CorePageUtils.filesSearch(filesNames, Paths.get("TEMP_files/TEMP_data/screens"));
        for (var entry : filePaths.entrySet()) {
//            System.out.println(entry.getKey() + ":     " + entry.getValue().toString());
            System.out.println(entry.getValue().toString());

        }
    }

    @Test
    void testToBase64() throws Exception {
        Path imgPath = Paths.get("TEMP_files/TEMP_data/screens/4537/448537.png.gz");
        Base64.Encoder encoder = Base64.getEncoder();
        String outFilePath = "TEMP_files/TEMP_out/decoded-img.png";
        String base64String;

        // Encoding png to base64
        try (InputStream stream = new GZIPInputStream(Files.newInputStream(imgPath))) {
            byte[] data = stream.readAllBytes();
            base64String = encoder.encodeToString(data);
        }
        System.out.println(base64String);

        // Decoding from base64 to png
        byte[] imageBytes = Base64.getDecoder().decode(base64String);

        try (FileOutputStream fout = new FileOutputStream(outFilePath)) {
            fout.write(imageBytes);
        }
    }

    @Test
    void dockerEasyocrTest() {
        System.out.println("docker test");
        DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("unix:///var/run/docker.sock") // Change if needed
                .build();

        // Docker client with explicit HTTP client
        DockerClient dockerClient = DockerClientBuilder
                .getInstance(config)
                .build();

        try {
            String containerName = "easocr-container";
            List<Container> containers = dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withNameFilter(List.of(containerName))
                    .exec();

            if (containers.isEmpty()) {
                System.out.println("Container does not exist: " + containerName);
                System.out.println("Creating container...");

                CreateContainerResponse containerResponse = dockerClient.createContainerCmd("easyocr-rest")
                        .withHostConfig(
                                HostConfig.newHostConfig()
                                        .withPortBindings(
                                                new PortBinding(Ports.Binding.bindPort(5000), new ExposedPort(5000))
                                        )
                                        .withDeviceRequests(
                                                List.of(
                                                        new DeviceRequest()
                                                                .withCapabilities(List.of(List.of("gpu")))
                                                                .withCount(-1)
                                                )
                                        )
                        )
                        .exec();

                System.out.println("Container created with ID: " + containerResponse.getId());
                containers = dockerClient.listContainersCmd()
                        .withShowAll(true)
                        .withIdFilter(List.of(containerResponse.getId()))
                        .exec();
            }

            for (var c : containers) {
                System.out.println("Container " + c.getNames().toString()  + "starting");
                dockerClient.startContainerCmd(c.getId()).exec();
                System.out.println("Status: " + c.getStatus());
            }
            dockerClient.close();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testCommLayer() throws Exception {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");

        String luaScriptPath = Files.readString(
                Path.of(DUUIComposer.class.getClassLoader()
                    .getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication_json.lua")
                    .toURI())
        );

        DUUILuaContext ctx = new DUUILuaContext();
        ctx.withGlobalLibrary("json", DUUIComposer.class.getClassLoader()
                .getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua")
                .toURI());

        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(luaScriptPath, "remote", ctx);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

//        CasIOUtils.save();
//        XmiCasDeserializer.deserialize();

        lua.serialize(jc, out, null);
        System.out.println(out.toString());
    }

    @Test
    void testLuaWithDriver() throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver);

        JCas jcas = JCasFactory.createJCas();

        composer.add(
                new DUUIRemoteDriver.Component("http://127.0.0.1:9714")
        );
    }


    @Test
    void testComposerEasyocr() throws Exception {
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(1)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIDockerDriver driver = new DUUIDockerDriver();
        composer.addDriver(driver);
        composer.add(new DUUIDockerDriver.Component("easyocr-rest").build());
//        driver.

        JCas jcas = JCasFactory.createJCas();
        jcas.setDocumentLanguage("en");
        jcas.setDocumentText("Text to process");

        composer.run(jcas, "processing-run");

    }

    @Test
    void testImageReader() throws IOException {
        ImageReader imgReader = new ImageReader("TEMP_files/TEMP_data/screens/4537");
        String base64String = ImageReader.imageToBase64("TEMP_files/TEMP_data/screens/4537/448537.png.gz");
        ImageReader.imageFromBase64(base64String, "TEMP_files/TEMP_out/decoded64.png");
    }

    @Test
    void testReadGzippedHTML() throws Exception {
        Path filePath = Paths.get("TEMP_files/TEMP_data/html/7053/1945217.html.gz");
        StringBuilder htmlString = new StringBuilder();

        try (FileInputStream fis = new FileInputStream(filePath.toString());
             GZIPInputStream gis = new GZIPInputStream(fis);
             InputStreamReader isr = new InputStreamReader(gis);
             BufferedReader br = new BufferedReader(isr))
        {
            String line;
            while ((line = br.readLine()) != null) {
                htmlString.append(line).append("\n");
            }
        }

        System.out.println(htmlString.toString());
    }

    /**
     * Reads in a serialized JCas object that contains an ImageBase64 annotation object and gets the image's width
     * and height.
     * @throws Exception
     */
    @Test
    void testGetDimensionsFromImageCas() throws Exception {
        JCas jcas = CorePageUtils.readJcasFromXmi("TEMP_files/TEMP_out/448537.xmi");

        for (ImageBase64 anno : JCasUtil.select(jcas, ImageBase64.class)) {
            System.out.println("Width | Height: " + anno.getWidth() + " | " + anno.getHeight());
        }
    }

    @Test
    void testLuaScriptSerialize() throws Exception {
        // Assumes there is an already generated output of the ImageReader which can be read in
        JCas jcas = CorePageUtils.readJcasFromXmi("TEMP_files/TEMP_out/448537.xmi");
        jcas.setDocumentLanguage("de");

        jcas.getDocumentText();

        // Read in lua script to String
        String luaScript = Files.readString(Path.of("src/main/java/org/texttechnologylab/DockerUnifiedUIMAInterface/io/reader/CoreReader/duui-easyocr-image-annotator.lua"));
        // Make the JsonLibrary available to the lua execution environment (context)
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        // Using the lua script (String) and the lua context, build a java object that can execute the functions in the lua script
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(luaScript, "remote", ctx);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jcas, out, null);
        System.out.println(out.toString());
    }

    /**
     * Assumes that an easyocr-rest docker container is running on port 9714 locally
     * @throws Exception
     */
    @Test
    void testEasyocrRemoteDriver() throws Exception {
        String inputDir = "TEMP_files/in/*.xmi";
        String outputDir = "TEMP_files/easyocr-out";

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(ctx);

        composer.addDriver(new DUUIRemoteDriver());
        composer.addDriver(new DUUIUIMADriver());

        composer.add(new DUUIRemoteDriver.Component("http://0.0.0.0:9714"));
        composer.add(
                new DUUIUIMADriver.Component(
                        AnalysisEngineFactory.createEngineDescription(
                                XmiWriter.class
                                , XmiWriter.PARAM_TARGET_LOCATION, outputDir
                                , XmiWriter.PARAM_PRETTY_PRINT, true
                                , XmiWriter.PARAM_OVERWRITE, true
                                , XmiWriter.PARAM_VERSION, "1.1"
                                , XmiWriter.PARAM_COMPRESSION, "NONE"
                        )
                )
        );

        composer.run(
                CollectionReaderFactory.createReaderDescription(
                        XmiReader.class,
                        XmiReader.PARAM_ADD_DOCUMENT_METADATA, false,
                        XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA, false,
                        XmiReader.PARAM_LENIENT, true,
                        XmiReader.PARAM_SOURCE_LOCATION, inputDir),
                        "run_serialize_json"
        );

        composer.shutdown();
    }

    @Test
    void testEasyocrDockerDriver() throws Exception {
        String inputDir = "TEMP_files/in/*.xmi";
        String outputDir = "TEMP_files/easyocr-out";

//        JCas jcas = JCasFactory.createJCas();

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        composer.addDriver(new DUUIDockerDriver().withTimeout(10000));
        composer.addDriver(new DUUIUIMADriver());

        composer.add(new DUUIDockerDriver.Component("easyocr-rest:latest").build());

        composer.add(
                new DUUIUIMADriver.Component(
                        AnalysisEngineFactory.createEngineDescription(
                                XmiWriter.class
                                , XmiWriter.PARAM_TARGET_LOCATION, outputDir
                                , XmiWriter.PARAM_PRETTY_PRINT, true
                                , XmiWriter.PARAM_OVERWRITE, true
                                , XmiWriter.PARAM_VERSION, "1.1"
                                , XmiWriter.PARAM_COMPRESSION, "NONE"
                        )
                )
        );

        composer.run(DUUICoreReaderTest.inputDirCas(inputDir));
        composer.shutdown();
    }

    @Test
    void tempTest() throws Exception {
        JCas jcas = JCasFactory.createJCas();

        FloatArray topLeft = new FloatArray(jcas, 2);
        topLeft.set(0, 12.0f);
        topLeft.set(1, 223.0f);

        FourPointBoundingBox bbox = new FourPointBoundingBox(jcas);
        bbox.setTopLeft(topLeft);
        bbox.addToIndexes();

        FourPointBoundingBox bbox2 = JCasUtil.selectSingle(jcas, FourPointBoundingBox.class);

        System.out.println(bbox2.getTopLeft());
    }


    /**
     * Runs the ImageReader in a pipeline with input images and output xmi files
     * @throws Exception
     */
    @Test
    void testImageReaderInPipeline() throws Exception {
        String inputDir = "TEMP_files/TEMP_data/screens/4537";
        String outputDir = "TEMP_files/out";

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(1)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        composer.addDriver(new DUUIUIMADriver());
        DUUIAsynchronousProcessor imageReader = new DUUIAsynchronousProcessor(
            new ImageReader(inputDir)
        );

        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(
                        XmiWriter.class
                        , XmiWriter.PARAM_TARGET_LOCATION, outputDir
                        , XmiWriter.PARAM_PRETTY_PRINT, true
                        , XmiWriter.PARAM_OVERWRITE, true
                        , XmiWriter.PARAM_VERSION, "1.1"
                        , XmiWriter.PARAM_COMPRESSION, "NONE"
                )
        ));

        composer.run(imageReader, "ImageReder-test");
        composer.shutdown();
    }

    @Test
    void testCoreReaderInPipeline() throws Exception {
        Path dummySource = Paths.get("./TEMP_files/TEMP_in");
        Path targetLocation = Paths.get("./TEMP_files/TEMP_out");

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(1)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        composer.addDriver(uimaDriver);



//        // Typesystem: CorePageTypes.xml
//        TypeSystemDescription corePageTypes = TypeSystemDescriptionFactory
//                .createTypeSystemDescriptionFromPath(
//                        "src/main/resources/org/texttechnologylab/types/CorePageTypes.xml"
//                );
//        CollectionReaderDescription reader =
//                CollectionReaderFactory.createReaderDescription(
//                        DUUICoreReader.class,
//                        corePageTypes,
//                        DUUICoreReader.PARAM_SOURCE_LOCATION, dummySource.toString(),
//                        DUUICoreReader.PARAM_PATTERNS, "[+]*.*"
//                );

        // Reader: CoreReader

        DUUIAsynchronousProcessor reader = new DUUIAsynchronousProcessor(
                new DUUICoreReader()
        );

        // XMI Writer
        composer.add(new DUUIUIMADriver.Component(
                AnalysisEngineFactory.createEngineDescription(
                        XmiWriter.class
                        , XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString()
                        , XmiWriter.PARAM_PRETTY_PRINT, true
                        , XmiWriter.PARAM_OVERWRITE, true
                        , XmiWriter.PARAM_VERSION, "1.1"
                        , XmiWriter.PARAM_COMPRESSION, "NONE"
                )
        ));

        composer.run(reader, "core_reader_test");
        composer.shutdown();
    }
}