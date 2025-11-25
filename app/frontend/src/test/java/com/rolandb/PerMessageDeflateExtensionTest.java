package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import org.java_websocket.exceptions.InvalidDataException;
import org.java_websocket.framing.DataFrame;
import org.java_websocket.framing.TextFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PerMessageDeflateExtensionTest {
    private PerMessageDeflateExtension extension;

    @BeforeEach
    public void setUp() {
        extension = new PerMessageDeflateExtension(-1, 0);
    }

    @Test
    public void testEncodeAndDecodeFrame() throws InvalidDataException {
        String originalMessage = "Hello World";
        ByteBuffer originalPayload = ByteBuffer.wrap(originalMessage.getBytes());
        DataFrame dataFrame = new TextFrame();
        dataFrame.setPayload(originalPayload);
        dataFrame.setFin(true);
        dataFrame.setRSV1(false);
        extension.encodeFrame(dataFrame);
        assertTrue(dataFrame.isRSV1());
        ByteBuffer compressedPayload = dataFrame.getPayloadData();

        DataFrame decodedFrame = new TextFrame();
        decodedFrame.setPayload(compressedPayload);
        decodedFrame.setFin(true);
        decodedFrame.setRSV1(true);
        extension.decodeFrame(decodedFrame);
        assertFalse(decodedFrame.isRSV1());

        byte[] decompressedBytes = new byte[decodedFrame.getPayloadData().remaining()];
        decodedFrame.getPayloadData().get(decompressedBytes);
        assertEquals(originalMessage, new String(decompressedBytes));
    }
}
