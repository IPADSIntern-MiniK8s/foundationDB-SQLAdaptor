package org.sjtu.se.ipads.fdbserver.tsdb.log;


import lombok.Getter;
import lombok.Setter;
import org.sjtu.se.ipads.fdbserver.service.UploadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
// TODO: need to consider more about ACID
public class LogEntry {
    private LogType type;
    private String timeStamp;
    private int carId;
    private int x;
    private int y;
    private int v_x;
    private int v_y;
    private int v_r;
    private int direction;
    private String img;
    private static int TIMESTAMP_LENGTH = 10;
    private Logger logger = LoggerFactory.getLogger(LogEntry.class);

    public LogEntry() {}

    public LogEntry(LogType logType, String timeStamp,
                    int carId, List<Integer> fields, String img) {
        this.type = logType;
        this.timeStamp = timeStamp;
        this.carId = carId;

        if (fields.size() != 6) {
            logger.error("the entry field length is illegal");
        } else {
            this.x = fields.get(0);
            this.y = fields.get(1);
            this.v_x = fields.get(2);
            this.v_y = fields.get(3);
            this.v_r = fields.get(4);
            this.direction = fields.get(5);
        }

        this.img = img;
    }

    /**
     * calculate the entry size
     * @return
     */
    public int size() {
        return 7 * Integer.BYTES + 2 + TIMESTAMP_LENGTH + img.length();
    }


    public static void intToArrayByLow(int n, byte[] bytes, int offset) {
        bytes[0 + offset] = (byte) (n & 0xff);
        bytes[1 + offset] = (byte) (n >>> 8 & 0xff);
        bytes[2 + offset] = (byte) (n >>> 16 & 0xff);
        bytes[3 + offset] = (byte) (n >>> 24 & 0xff);
    }

    public static int byteArrayToInt(byte[] b, int offset) {
        int n = 0;
        int len = b.length;
        if (len >= offset + 4) {
            int byte0 = b[offset] & 0xff;
            int byte1 = b[offset + 1] & 0xff;
            int byte2 = b[offset + 2] & 0xff;
            int byte3 = b[offset + 3] & 0xff;
            n = byte0 | byte1 << 8 | byte2 << 16 | byte3 << 24;
        }
        return n;
    }


    public static void charToByte(char c, byte[] bytes, int offset) {
        bytes[offset + 0] = (byte) ((c & 0xFF00) >> 8);
        bytes[offset + 1] = (byte) (c & 0xFF);
    }


    public static char byteToChar(byte[] bytes, int offset) {
        char c = (char) (((bytes[0 + offset] & 0xFF) << 8) | (bytes[1 + offset] & 0xFF));
        return c;
    }


    public static byte[] strToByteArray(String str) {
        if (str == null) {
            return null;
        }
        byte[] byteArray = str.getBytes();
        return byteArray;
    }

    public static String byteArrayToStr(byte[] byteArray, int offset, int length) {
        if (byteArray == null) {
            return null;
        }
        String str = new String(byteArray, offset, length);
        return str;
    }


    /**
     * serialize the entry to a byte array for storage
     * @return
     */
    public Map.Entry<Integer, byte[]> serialize() {
        int size = this.size();
        byte[] result = new byte[size + Integer.BYTES];
        int offset = 0;
        // size(4) | Type(2) | TIMESTAMP (10) | CarId(4) | x(4) | y(4) | v_x(4) | v_y(4) | v_r(4) | direction(4) | img(variable)
        intToArrayByLow(size, result, offset);
        offset += Integer.BYTES;
        charToByte(this.type.getType(), result, offset);
        offset += 2;
        byte[] newBytes = strToByteArray(this.timeStamp);
        System.arraycopy(newBytes, 0, result, offset, newBytes.length);
        offset += TIMESTAMP_LENGTH;
        intToArrayByLow(this.carId, result, offset);
        offset += Integer.BYTES;
        intToArrayByLow(this.x, result, offset);
        offset += Integer.BYTES;
        intToArrayByLow(this.y, result, offset);
        offset += Integer.BYTES;
        intToArrayByLow(this.v_x, result, offset);
        offset += Integer.BYTES;
        intToArrayByLow(this.v_y, result, offset);
        offset += Integer.BYTES;
        intToArrayByLow(this.v_r, result, offset);
        offset += Integer.BYTES;
        intToArrayByLow(this.direction, result, offset);
        offset += Integer.BYTES;
        byte[] img = strToByteArray(this.img);
        System.arraycopy(img, 0, result, offset, img.length);

        return new AbstractMap.SimpleEntry<>(size, result);
    }


    /**
     * deserialize the byte array to a byte array for storage
     * @return
     */
    void deserialize(byte[] bytes, int size) {
        // size(4) | Type(2) | TIMESTAMP (13) | CarId(4) | x(4) | y(4) | v_x(4) | v_y(4) | v_r(4) | direction(4) | img(variable)
        int offset = 0;
        char typeChar = byteToChar(bytes, offset);
        if (typeChar == '0') {
            this.type = LogType.INSERT;
        } else {
            this.type = LogType.DELETE;
        }

        offset += 2;
        this.timeStamp = byteArrayToStr(bytes, offset, TIMESTAMP_LENGTH);
        offset += TIMESTAMP_LENGTH;
        this.carId = byteArrayToInt(bytes, offset);
        offset += Integer.BYTES;
        this.x = byteArrayToInt(bytes, offset);
        offset += Integer.BYTES;
        this.y = byteArrayToInt(bytes, offset);
        offset += Integer.BYTES;
        this.v_x = byteArrayToInt(bytes, offset);
        offset += Integer.BYTES;
        this.v_y = byteArrayToInt(bytes, offset);
        offset += Integer.BYTES;
        this.v_r = byteArrayToInt(bytes, offset);
        offset += Integer.BYTES;
        this.direction = byteArrayToInt(bytes, offset);
        offset += Integer.BYTES;
        int imgLength = size - offset;
        System.out.println("img length: " + imgLength);
        this.img = byteArrayToStr(bytes, offset, imgLength);
    }

}
