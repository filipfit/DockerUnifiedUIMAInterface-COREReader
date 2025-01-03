package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.CoreReader;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.ImageBase64;

import java.lang.reflect.InvocationTargetException;

public class DUUICoreReaderLuaUtils {
    public static String getImageBase64(JCas jcas) {
        ImageBase64 anno = JCasUtil.selectSingle(jcas, ImageBase64.class);
        return anno.getBase64String();
    }

    public static int getImageWidth(JCas jcas) {
        ImageBase64 anno = JCasUtil.selectSingle(jcas, ImageBase64.class);
        return anno.getWidth();
    }

    public static int getImageHeight(JCas jcas) {
        ImageBase64 anno = JCasUtil.selectSingle(jcas, ImageBase64.class);
        return anno.getHeight();
    }
}
