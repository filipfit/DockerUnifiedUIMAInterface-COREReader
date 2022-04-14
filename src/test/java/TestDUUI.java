import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.CASCompleteSerializer;
import org.apache.uima.cas.impl.CASMgrSerializer;
import org.apache.uima.cas.impl.CASSerializer;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XmlCasSerializer;
import org.hucompute.textimager.uima.type.Sentiment;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaSandbox;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestDUUI {
    @Test
    public void LuaBaseTest() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
        System.out.println(out.toString());
    }

    @Test
    public void LuaLibTest() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication_json.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
        System.out.println(out.toString());
    }

    @Test
    public void LuaLibTestSandboxInstructionOverflow() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/only_loaded_classes.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox((new DUUILuaSandbox())
                .withLimitInstructionCount(1));
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());

        assertThrows(RuntimeException.class, () -> {
                    DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
                });
    }

    @Test
    public void LuaLibTestSandboxInstructionOk() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/only_loaded_classes.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox((new DUUILuaSandbox())
                .withLimitInstructionCount(10000));
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
        assertEquals(out.toString(),"");
    }

    @Test
    public void LuaLibTestSandboxForbidLoadJavaClasses() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/uima_xmi_communication.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox());
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        assertThrows(RuntimeException.class,()->{DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);});
    }

    @Test
    public void LuaLibTestSandboxForbidLoadJavaIndirectCall() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox());
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        assertThrows(RuntimeException.class,()->{
            DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            lua.serialize(jc,out);
        });
    }

    @Test
    public void LuaLibTestSandboxEnableLoadJavaIndirectCall() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox().withAllJavaClasses(true));
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
    }

    @Test
    public void LuaLibTestSandboxSelectiveJavaClasses() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox().withAllowedJavaClass("org.apache.uima.cas.impl.XmiCasSerializer")
                .withAllowedJavaClass("java.lang.String"));
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lua.serialize(jc,out);
    }

    @Test
    public void LuaLibTestSandboxFailureSelectiveJavaClasses() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        jc.setDocumentText("Hallo Welt!");
        jc.setDocumentLanguage("de");
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/use_java_indirect.lua").toURI()));

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withSandbox(new DUUILuaSandbox().withAllowedJavaClass("org.apache.uima.cas.impl.XmiCasSerializer"));
        ctxt.withGlobalLibrary("json",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/json.lua").toURI());


        assertThrows(RuntimeException.class,()-> {
            DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        });
    }

    @Test
    public void LuaLargeSerialize() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_json.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc,out);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Lua JSON in %d ms time," +
                " total bytes %d\n",end-start,out.toString().length());
        JSONArray arr = new JSONArray(out.toString());

        assertEquals(expectedNumberOfTokens,arr.getJSONArray(1).length());
        assertEquals(expectedNumberOfTokens,JCasUtil.select(jc,Token.class).size());
    }

    @Test
    public void LuaLargeSerializeMsgpack() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);


        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }

        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_msgpack.lua").toURI()));
        DUUILuaContext ctxt = new DUUILuaContext();
        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val,"remote",ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc,out);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Lua MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n",end-start,out.toString().length(),expectedNumberOfTokens);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(out.toByteArray());
        String text = unpacker.unpackString();
        int numTokensTimes2_2 = unpacker.unpackArrayHeader();
        assertEquals(expectedNumberOfTokens*2,numTokensTimes2_2);
    }

    @Test
    public void JavaXMLSerialize() throws UIMAException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        XmlCasSerializer.serialize(jc.getCas(),null,out);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize full XML in %d ms time," +
                " total bytes %d\n",end-start,out.toString().length());
        Files.write(Path.of("python_benches","large_xmi.xml"),out.toByteArray());
    }

    @Test
    public void JavaBinarySerialize() throws UIMAException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        CasIOUtils.save(jc.getCas(),out, SerialFormat.BINARY);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize binary JCas in %d ms time," +
                " total bytes %d\n",end-start,out.toString().length());
    }

    @Test
    public void JavaSerializeMsgpack() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packString(jc.getDocumentText());
        packer.packArrayHeader(JCasUtil.select(jc,Token.class).size()*2);
        for(Token t : JCasUtil.select(jc,Token.class)) {
            packer.packInt(t.getBegin());
            packer.packInt(t.getEnd());
        }
        packer.close();
        out.write(packer.toByteArray());

        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Java MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n",end-start,out.toString().length(),expectedNumberOfTokens);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(out.toByteArray());
        String text = unpacker.unpackString();
        int numTokensTimes2_2 = unpacker.unpackArrayHeader();
        assertEquals(expectedNumberOfTokens*2,numTokensTimes2_2);
    }

    @Test
    public void JavaSerializeJSON() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long start = System.currentTimeMillis();
        JSONArray begin = new JSONArray();
        JSONArray endt = new JSONArray();

        for(Token t : JCasUtil.select(jc,Token.class)) {
            begin.put(t.getBegin());
            endt.put(t.getEnd());
        }
        JSONArray arr2 = new JSONArray();
        arr2.put(jc.getDocumentText());
        arr2.put(begin);
        arr2.put(endt);
        out.write(arr2.toString().getBytes(StandardCharsets.UTF_8));
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Java JSON in %d ms time," +
                " total bytes %d, total tokens %d\n",end-start,out.toString().length(),expectedNumberOfTokens);
        JSONArray arr = new JSONArray(out.toString());
        assertEquals(expectedNumberOfTokens,arr.getJSONArray(1).length());
        assertEquals(expectedNumberOfTokens,JCasUtil.select(jc,Token.class).size());
    }

    @Test
    public void LuaMsgPackNative() throws UIMAException, CompressorException, IOException, SAXException, URISyntaxException {
        JCas jc = JCasFactory.createJCas();
        String val2 = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/large_texts/1000.txt").toURI()));
        jc.setDocumentText(val2);
        jc.setDocumentLanguage("de");
        AnalysisEngineDescription desc = AnalysisEngineFactory.createEngineDescription(BreakIteratorSegmenter.class);
        SimplePipeline.runPipeline(jc,desc);

        int expectedNumberOfTokens = 0;
        for(Token t : JCasUtil.select(jc,Token.class)) {
            expectedNumberOfTokens+=1;
        }

        DUUILuaContext ctxt = new DUUILuaContext();
        ctxt.withGlobalLibrary("nativem",DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/lua_stdlib/MessagePack.lua").toURI());
        String val = Files.readString(Path.of(DUUIComposer.class.getClassLoader().getResource("org/texttechnologylab/DockerUnifiedUIMAInterface/rust_communication_msgpack_native.lua").toURI()));

        DUUILuaCommunicationLayer lua = new DUUILuaCommunicationLayer(val, "remote", ctxt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long start = System.currentTimeMillis();
        lua.serialize(jc,out);
        long end = System.currentTimeMillis();
        System.out.printf("Serialize large Lua Native MsgPack in %d ms time," +
                " total bytes %d, total tokens %d\n",end-start,out.toString().length(),expectedNumberOfTokens);
    }

//    @Test
//    public void XMIWriterTest() throws ResourceInitializationException, IOException, SAXException {
//
//        int iWorkers = 8;
//
//        DUUIComposer composer = new DUUIComposer().withWorkers(iWorkers);
//
//        DUUIUIMADriver uima_driver = new DUUIUIMADriver();
//
//        composer.addDriver(uima_driver);
//
//        // UIMA Driver handles all native UIMA Analysis Engine Descriptions
//        composer.add(new DUUIUIMADriver.Component(
//                AnalysisEngineFactory.createEngineDescription(StanfordPosTagger.class)
//        ).withScale(iWorkers), DUUIUIMADriver.class);
//        composer.add(new DUUIUIMADriver.Component(
//                AnalysisEngineFactory.createEngineDescription(StanfordParser.class)
//        ).withScale(iWorkers), DUUIUIMADriver.class);
//        composer.add(new DUUIUIMADriver.Component(
//                AnalysisEngineFactory.createEngineDescription(StanfordNamedEntityRecognizer.class)
//        ).withScale(iWorkers), DUUIUIMADriver.class);
//
//        composer.add(new DUUIUIMADriver.Component(
//                AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
//                        XmiWriter.PARAM_TARGET_LOCATION, "/tmp/output/",
//                        XmiWriter.PARAM_PRETTY_PRINT, true,
//                        XmiWriter.PARAM_OVERWRITE, true,
//                        XmiWriter.PARAM_VERSION, "1.1",
//                        XmiWriter.PARAM_COMPRESSION, "GZIP"
//                        )
//        ).withScale(iWorkers), DUUIUIMADriver.class);
//
//        try {
//            composer.run(createReaderDescription(XmiReaderModified.class,
//                    XmiReader.PARAM_SOURCE_LOCATION, "/resources/public/abrami/Zobodat/xmi/txt/**.xmi.gz",
//                    XmiWriter.PARAM_OVERWRITE, false
//                    //XmiReader.PARAM_LANGUAGE, LanguageToolSegmenter.PARAM_LANGUAGE)
//            ));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }

}
