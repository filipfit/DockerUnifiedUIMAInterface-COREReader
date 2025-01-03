package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.CoreReader;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XmlCasDeserializer;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class CorePageUtils {

    public static String getBaseName(String fileName) {
        int index = fileName.indexOf('.');
        return (index == -1) ? fileName : fileName.substring(0, index);
    }

    /**
     * Searches for files whose file name is in fileNames. Starts search in startDir. File names must contain only files
     * without any of their extension. E.g. "image.png.gz" should be "image".
     *
     * @param fileNames List of just filenames without any extensions
     * @param startDir  Directory to start searching from
     * @return List of full paths to the found files
     * @throws IOException
     */
    public static Map<String, Path> filesSearch(Set<String> fileNames, Path startDir) throws IOException {
        return Files.walk(startDir)
                .filter(Files::isRegularFile)
                .filter(path -> {
                    String f = CorePageUtils.getBaseName(path.getFileName().toString());
                    return fileNames.contains(f);
                })
                .collect(Collectors.toMap(
                        path -> CorePageUtils.getBaseName(path.getFileName().toString()),
                        path -> path
                ));
    }

    public static String pngToBase64(Path imagePath) throws IOException {
        Base64.Encoder encoder = Base64.getEncoder();
        String base64String;

        try (InputStream stream = new GZIPInputStream(Files.newInputStream(imagePath))) {
            byte[] data = stream.readAllBytes();
            base64String = encoder.encodeToString(data);
        }
        return base64String;
    }

    public static String readGzippedHTML(Path gzippedFilePath) throws IOException {
        StringBuilder htmlString = new StringBuilder();
        try (GZIPInputStream gzStream = new GZIPInputStream(Files.newInputStream(gzippedFilePath));
             InputStreamReader reader = new InputStreamReader(gzStream, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(reader)
        ) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                htmlString.append(line).append("\n");
            }
        }
        return htmlString.toString();
    }

    /**
     * Reads a serialized CAS from the provided xmi-file into a JCas object.
     * @param xmiFilePath
     * @return  JCas with the deserialized content from the provided xmi-file
     * @throws ResourceInitializationException
     * @throws CASException
     */
    public static JCas readJcasFromXmi(String xmiFilePath) throws ResourceInitializationException, CASException {
        File xmiFile = new File(xmiFilePath);
        JCas jcas = JCasFactory.createJCas();

        try (FileInputStream stream = new FileInputStream(xmiFile)) {
            XmlCasDeserializer.deserialize(stream, jcas.getCas(), true);
        } catch (IOException | SAXException e) {
            throw new RuntimeException(e);
        }

        return jcas;
    }
}
