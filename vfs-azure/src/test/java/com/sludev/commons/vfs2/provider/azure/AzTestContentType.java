package com.sludev.commons.vfs2.provider.azure;

import org.apache.tika.Tika;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class AzTestContentType {

    private static Tika tika;


    @BeforeClass
    public static void doSetUp() {

        tika = new Tika();
    }


    @Test
    public void testMP4() {

        String output = tika.detect("abc.mp4");
        assertEquals("video/mp4", output);
    }


    @Test
    public void testMOV() {

        String output = tika.detect("abc.mov");
        assertEquals("video/quicktime", output);
    }


    @Test
    public void testTXT() {

        String output = tika.detect("abc.txt");
        assertEquals("text/plain", output);
    }


    @Test
    public void testWithoutExt() {

        String output = tika.detect("abc");
        assertEquals("application/octet-stream", output);
    }
}
