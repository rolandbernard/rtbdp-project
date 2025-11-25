package com.rolandb;

import static java.util.zip.Deflater.DEFAULT_COMPRESSION;
import static java.util.zip.Deflater.NO_FLUSH;
import static java.util.zip.Deflater.SYNC_FLUSH;
import static org.java_websocket.extensions.ExtensionRequestData.parseExtensionRequest;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.java_websocket.exceptions.InvalidDataException;
import org.java_websocket.extensions.CompressionExtension;
import org.java_websocket.extensions.ExtensionRequestData;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.framing.ContinuousFrame;
import org.java_websocket.framing.DataFrame;
import org.java_websocket.framing.Framedata;

/**
 * I have to implement this here since the default implementation in this
 * library is unfortunately broken. See the discussion at for more details
 * https://github.com/TooTallNate/Java-WebSocket/issues/1496.
 */
public class PerMessageDeflateExtension extends CompressionExtension {
    public static final String PREMESSAGE_DEFLATE = "permessage-deflate";

    public static final String CLIENT_NO_CONTEXT_TAKEOVER = "client_no_context_takeover";
    public static final String SERVER_NO_CONTEXT_TAKEOVER = "server_no_context_takeover";
    public static final String CLIENT_MAX_WINDOW_BITS = "client_max_window_bits";
    public static final String SERVER_MAX_WINDOW_BITS = "server_max_window_bits";
    public static final int MINIMUM_MAX_WINDOW_BITS = 8;
    public static final int MAXIMUM_MAX_WINDOW_BITS = 15;
    // This is the default for Deflate implementation in Java, and it does not seem
    // like it can be changed. For this reason we will have to fall back to
    // uncompressed if the client does not support it.
    public static final int DEFAULT_MAX_WINDOW_BITS = 15;

    public static final byte[] FINAL_DEFLATE_BLOCK = new byte[] { 0x00, 0x00, (byte) 0xff, (byte) 0xff };
    public static final byte[] EMPTY_DEFLATE_BLOCK = new byte[] { 0x00 };
    private static final int CHUNK_SIZE = 8192;

    private final int compressionLevel;
    private final Deflater compressor;
    private final Inflater decompressor;

    private boolean clientNoContextTakeover;
    private boolean serverNoContextTakeover;
    private int clientMaxWindowBits;
    private int serverMaxWindowBits;

    private boolean resetCompressor;
    private boolean resetDecompressor;

    private int compressionThreshold;
    private boolean isCompressing;
    private boolean isDecompressing;

    public PerMessageDeflateExtension() {
        this(DEFAULT_COMPRESSION, 64);
    }

    public PerMessageDeflateExtension(int compressionLevel, int compressionThreshold) {
        this.compressionLevel = compressionLevel;
        this.compressionThreshold = compressionThreshold;
        compressor = new Deflater(compressionLevel, true);
        decompressor = new Inflater(true);
        clientNoContextTakeover = false;
        serverNoContextTakeover = false;
        clientMaxWindowBits = MAXIMUM_MAX_WINDOW_BITS;
        serverMaxWindowBits = MAXIMUM_MAX_WINDOW_BITS;
        resetCompressor = false;
        resetDecompressor = false;
        isCompressing = false;
        isDecompressing = false;
    }

    @Override
    public void decodeFrame(Framedata inputFrame) throws InvalidDataException {
        if (!(inputFrame instanceof DataFrame) || (!isDecompressing && inputFrame instanceof ContinuousFrame)) {
            return;
        }
        if (inputFrame.isRSV1()) {
            isDecompressing = true;
        }
        if (isDecompressing) {
            DataFrame dataFrame = (DataFrame) inputFrame;
            byte[] decompressed = decompress(dataFrame.getPayloadData(), dataFrame.isFin());
            dataFrame.setPayload(ByteBuffer.wrap(decompressed));
            dataFrame.setRSV1(false);
            if (dataFrame.isFin()) {
                isDecompressing = false;
                if (resetDecompressor) {
                    decompressor.reset();
                }
            }
        }
    }

    private byte[] decompress(ByteBuffer buffer, boolean isFinal) throws InvalidDataException {
        ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
        try {
            decompressInto(buffer, decompressed);
            if (isFinal) {
                decompressInto(ByteBuffer.wrap(FINAL_DEFLATE_BLOCK), decompressed);
            }
        } catch (DataFormatException e) {
            throw new InvalidDataException(CloseFrame.POLICY_VALIDATION, e.getMessage());
        }
        return decompressed.toByteArray();
    }

    private void decompressInto(ByteBuffer buffer, ByteArrayOutputStream decompressed) throws DataFormatException {
        decompressor.setInput(buffer);
        byte[] chunk = new byte[CHUNK_SIZE];
        while (!decompressor.finished()) {
            int length = decompressor.inflate(chunk);
            if (length > 0) {
                decompressed.write(chunk, 0, length);
            } else {
                break;
            }
        }
    }

    @Override
    public void encodeFrame(Framedata inputFrame) {
        if (!(inputFrame instanceof DataFrame) || (!isCompressing && inputFrame instanceof ContinuousFrame)) {
            return;
        }
        if (inputFrame.getPayloadData().remaining() >= compressionThreshold) {
            isCompressing = true;
        }
        if (isCompressing) {
            DataFrame dataFrame = (DataFrame) inputFrame;
            byte[] compressed = compress(dataFrame.getPayloadData(), dataFrame.isFin());
            dataFrame.setPayload(ByteBuffer.wrap(compressed));
            if (!(dataFrame instanceof ContinuousFrame)) {
                dataFrame.setRSV1(true);
            }
            if (dataFrame.isFin()) {
                isCompressing = false;
                if (resetCompressor) {
                    compressor.reset();
                }
            }
        }
    }

    private byte[] compress(ByteBuffer buffer, boolean isFinal) {
        if (!buffer.hasRemaining() && isFinal) {
            return EMPTY_DEFLATE_BLOCK;
        }
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        compressInto(buffer, compressed);
        return isFinal
                ? removeFinalBlock(compressed.toByteArray())
                : compressed.toByteArray();
    }

    private void compressInto(ByteBuffer buffer, ByteArrayOutputStream compressed) {
        compressor.setInput(buffer);
        int flush = NO_FLUSH;
        byte[] chunk = new byte[CHUNK_SIZE];
        while (!compressor.finished()) {
            int length = compressor.deflate(chunk, 0, chunk.length, flush);
            if (length > 0) {
                compressed.write(chunk, 0, length);
            } else if (flush == NO_FLUSH) {
                flush = SYNC_FLUSH;
            } else {
                break;
            }
        }
    }

    private byte[] removeFinalBlock(byte[] input) {
        byte[] block = FINAL_DEFLATE_BLOCK;
        if (input.length < block.length) {
            return input;
        }
        for (int i = 0; i < block.length; i++) {
            if (input[input.length - block.length + i] != block[i]) {
                return input;
            }
        }
        return Arrays.copyOf(input, input.length - block.length);
    }

    @Override
    public boolean acceptProvidedExtensionAsServer(String inputExtension) {
        for (String extensionRequest : inputExtension.split(",")) {
            ExtensionRequestData extensionRequestData = parseExtensionRequest(extensionRequest);
            if (PREMESSAGE_DEFLATE.equalsIgnoreCase(extensionRequestData.getExtensionName())
                    && acceptExtensionParametersAsServer(extensionRequestData)) {
                return true;
            }
        }
        return false;
    }

    private boolean acceptExtensionParameters(ExtensionRequestData extensionRequestData) {
        for (Map.Entry<String, String> parameter : extensionRequestData.getExtensionParameters().entrySet()) {
            if (CLIENT_NO_CONTEXT_TAKEOVER.equalsIgnoreCase(parameter.getKey())) {
                clientNoContextTakeover = true;
            } else if (SERVER_NO_CONTEXT_TAKEOVER.equalsIgnoreCase(parameter.getKey())) {
                serverNoContextTakeover = true;
            } else if (CLIENT_MAX_WINDOW_BITS.equalsIgnoreCase(parameter.getKey())) {
                if (!parameter.getValue().isEmpty()) {
                    try {
                        clientMaxWindowBits = Integer.parseInt(parameter.getValue());
                    } catch (NumberFormatException e) {
                        return false;
                    }
                    if (clientMaxWindowBits < MINIMUM_MAX_WINDOW_BITS
                            || clientMaxWindowBits > MAXIMUM_MAX_WINDOW_BITS) {
                        return false;
                    }
                }
            } else if (SERVER_MAX_WINDOW_BITS.equalsIgnoreCase(parameter.getKey())) {
                if (!parameter.getValue().isEmpty()) {
                    try {
                        serverMaxWindowBits = Integer.parseInt(parameter.getValue());
                    } catch (NumberFormatException e) {
                        return false;
                    }
                    if (serverMaxWindowBits < MINIMUM_MAX_WINDOW_BITS
                            || serverMaxWindowBits > MAXIMUM_MAX_WINDOW_BITS) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
        return true;
    }

    private boolean acceptExtensionParametersAsServer(ExtensionRequestData extensionRequestData) {
        if (!acceptExtensionParameters(extensionRequestData)) {
            return false;
        }
        if (serverMaxWindowBits < DEFAULT_MAX_WINDOW_BITS || clientMaxWindowBits > DEFAULT_MAX_WINDOW_BITS) {
            // We can't change these parameters unfortunately.
            return false;
        }
        resetCompressor = serverNoContextTakeover;
        resetDecompressor = clientNoContextTakeover;
        return true;
    }

    @Override
    public boolean acceptProvidedExtensionAsClient(String inputExtension) {
        for (String extensionRequest : inputExtension.split(",")) {
            ExtensionRequestData extensionRequestData = parseExtensionRequest(extensionRequest);
            if (PREMESSAGE_DEFLATE.equalsIgnoreCase(extensionRequestData.getExtensionName())) {
                return acceptExtensionParametersAsClient(extensionRequestData);
            }
        }
        return false;
    }

    private boolean acceptExtensionParametersAsClient(ExtensionRequestData extensionRequestData) {
        if (!acceptExtensionParameters(extensionRequestData)) {
            return false;
        }
        if (clientMaxWindowBits < DEFAULT_MAX_WINDOW_BITS || serverMaxWindowBits > DEFAULT_MAX_WINDOW_BITS) {
            // We can't change these parameters unfortunately.
            return false;
        }
        resetCompressor = clientNoContextTakeover;
        resetDecompressor = serverNoContextTakeover;
        return true;
    }

    @Override
    public String getProvidedExtensionAsClient() {
        return PREMESSAGE_DEFLATE
                + (clientNoContextTakeover ? "; " + CLIENT_NO_CONTEXT_TAKEOVER : "")
                + (serverNoContextTakeover ? "; " + SERVER_NO_CONTEXT_TAKEOVER : "")
                + (clientMaxWindowBits != DEFAULT_MAX_WINDOW_BITS
                        ? "; " + CLIENT_MAX_WINDOW_BITS + "=" + clientMaxWindowBits
                        : "")
                + (serverMaxWindowBits != DEFAULT_MAX_WINDOW_BITS
                        ? "; " + SERVER_MAX_WINDOW_BITS + "=" + serverMaxWindowBits
                        : "");
    }

    @Override
    public String getProvidedExtensionAsServer() {
        return PREMESSAGE_DEFLATE
                + (clientNoContextTakeover ? "; " + CLIENT_NO_CONTEXT_TAKEOVER : "")
                + (serverNoContextTakeover ? "; " + SERVER_NO_CONTEXT_TAKEOVER : "")
                + (clientMaxWindowBits != DEFAULT_MAX_WINDOW_BITS
                        ? "; " + CLIENT_MAX_WINDOW_BITS + "=" + clientMaxWindowBits
                        : "")
                + (serverMaxWindowBits != DEFAULT_MAX_WINDOW_BITS
                        ? "; " + SERVER_MAX_WINDOW_BITS + "=" + serverMaxWindowBits
                        : "");
    }

    @Override
    public PerMessageDeflateExtension copyInstance() {
        PerMessageDeflateExtension clone = new PerMessageDeflateExtension(compressionLevel, compressionThreshold);
        clone.clientNoContextTakeover = clientNoContextTakeover;
        clone.serverNoContextTakeover = serverNoContextTakeover;
        clone.clientMaxWindowBits = clientMaxWindowBits;
        clone.serverMaxWindowBits = serverMaxWindowBits;
        return clone;
    }

    @Override
    public void reset() {
        isCompressing = false;
        isDecompressing = false;
    }
}
