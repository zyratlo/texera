package edu.uci.ics.textdb.exp.source.file;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.POIXMLProperties;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextShape;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Created by junm5 on 5/3/17.
 */
public class TextExtractor {

    public static String extractCommonFile(Path path) {
        if (path == null) {
            return null;
        }
        try {
            return new String(Files.readAllBytes(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * use pdfbox to extract data from pdf document.
     * This also can be done using Tika lib.
     *
     * @param path
     * @return
     */
    public static String extractPDFFile(Path path) {
        if (path == null) {
            return null;
        }
        PDDocument doc = null;
        try {
            doc = PDDocument.load(new File(path.toString()));
            return new PDFTextStripper().getText(doc);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (doc != null) {
                try {
                    doc.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * use java poi to extract data from PPT file
     * attention: tika may conflict with poi
     *
     * @param path
     * @return
     */
    public static String extractPPTFile(Path path) {
        FileInputStream inputStream = null;
        StringBuffer res = new StringBuffer();

        try {
            inputStream = new FileInputStream(path.toString());
            XMLSlideShow ppt = new XMLSlideShow(inputStream);
            for (XSLFSlide slide : ppt.getSlides()) {
                List<XSLFShape> shapes = slide.getShapes();
                for (XSLFShape shape : shapes) {
                    if (shape instanceof XSLFTextShape) {
                        XSLFTextShape textShape = (XSLFTextShape) shape;
                        String text = textShape.getText();
                        res.append(text);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return res.toString();
    }

//    public static String extractPPTFile(Path path) {
//        if (path == null) {
//            return null;
//        }
//        try {
//            File file = new File(path.toString());
//            OfficeParser autoDetectParser = new OfficeParser();
//            ContentHandler handler = new WriteOutContentHandler(new StringWriter());
//            autoDetectParser.parse(new FileInputStream(file), handler, new Metadata(), new ParseContext());
//            return handler.toString();
//        } catch (Exception exp) {
//            exp.printStackTrace();
//        }
//        return null;
//
//    }

}
