package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.CoreReader;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.annotation.ImageBase64;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;


public class ImageReader implements DUUICollectionReader
{
    private String inputDir;
    private int initialQueueSize; // Amount of files to process in the _inputDir
    private ConcurrentLinkedQueue<String> filePaths;
    private AtomicInteger documentNumber;
    private AdvancedProgressMeter progress = null;
    private String currentFilePath = "";

    public ImageReader(String inputDir) throws IOException {
        this.inputDir = inputDir;
        this.filePaths = new ConcurrentLinkedQueue<>();
        this.documentNumber = new AtomicInteger(0);


        DirectoryStream<Path> dirStream = Files.newDirectoryStream(Path.of(this.inputDir));

        // Add all PNG files to _files
        for (Path entry : dirStream) {
            if (Files.isRegularFile(entry)
                    // TODO Temp
//                    && entry.endsWith(".png")
            ) {
                filePaths.add(entry.toString());
            }
        }
        initialQueueSize = filePaths.size();
        progress = new AdvancedProgressMeter(this.initialQueueSize);
    }

    public static byte[] decompressGzip(String filepath) throws IOException {
        InputStream stream = new GZIPInputStream(Files.newInputStream(Path.of(filepath)));
        return stream.readAllBytes();
    }

    public static int[] imageDimensions(byte[] imageBytes) throws IOException {
        ByteArrayInputStream imageByteStream = new ByteArrayInputStream(imageBytes);
        BufferedImage image = ImageIO.read(imageByteStream);
        return new int[] {image.getWidth(), image.getHeight()};
    }

    public static String imageToBase64(byte[] imgBytes) {
        Base64.Encoder encoder = Base64.getEncoder();
        return encoder.encodeToString(imgBytes);
    }

    public static String imageToBase64(String imgPath) {
        byte[] imgBytes = new byte[] {};
        try {
            imgBytes = decompressGzip(imgPath);
        } catch (IOException e) {
            System.out.println("[ImageReader.imageToBase64] Could not open image file");
            e.printStackTrace();
        }

        return imageToBase64(imgBytes);
    }

    public static void imageFromBase64(String base64Img, String outputPath) {
        byte[] imageBytes = Base64.getDecoder().decode(base64Img);

        try (FileOutputStream fout = new FileOutputStream(outputPath)) {
            fout.write(imageBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addMetadata(JCas jcas) {
        DocumentMetaData dmd = new DocumentMetaData(jcas);
        // Filename without any extensions, removes everything after the first "." in the filename
        String filenameNoExt = Path.of(this.currentFilePath).getFileName().toString().replaceFirst("[.].+", "");

        dmd.setDocumentId(filenameNoExt);
        dmd.setDocumentTitle(filenameNoExt);
        dmd.setDocumentUri(Path.of(this.currentFilePath).toAbsolutePath().toString());
        dmd.addToIndexes();
    }

    @Override
    public AdvancedProgressMeter getProgress() {
        return this.progress;
    }

    @Override
    public void getNextCas(JCas pCas) {
        this.currentFilePath = filePaths.poll();
        // Number of document in the queue that is bein currently processed
        int processedDocumentNumber = this.documentNumber.addAndGet(1);
        int width = 0;
        int height = 0;
        byte[] imgBytes = new byte[] {};
        String imgBase64Encoding;

        try {
            imgBytes = ImageReader.decompressGzip(this.currentFilePath);
            int[] imageDimensions = ImageReader.imageDimensions(imgBytes);
            width = imageDimensions[0];
            height = imageDimensions[1];
        } catch (IOException e) {
            System.out.println("[ImageReader.getNextCas] Could not decompress file: " + this.currentFilePath);
            e.printStackTrace();
        }

        pCas.setDocumentText(ImageReader.imageToBase64(imgBytes));

        if (JCasUtil.select(pCas, DocumentMetaData.class).isEmpty()) { this.addMetadata(pCas); }

        ImageBase64 imgAnno = new ImageBase64(pCas);
        imgAnno.setWidth(width);
        imgAnno.setHeight(height);
//        imgAnno.setBase64String(ImageReader.imageToBase64(imgBytes));
        imgAnno.addToIndexes();


        this.progress.setDone(processedDocumentNumber);
        this.progress.setLeft(initialQueueSize - processedDocumentNumber);
    }

    @Override
    public boolean hasNext() {
        return !filePaths.isEmpty();
    }

    @Override
    public long getSize() {
        return filePaths.size();
    }

    @Override
    public long getDone() {
        return documentNumber.get();
    }
}
