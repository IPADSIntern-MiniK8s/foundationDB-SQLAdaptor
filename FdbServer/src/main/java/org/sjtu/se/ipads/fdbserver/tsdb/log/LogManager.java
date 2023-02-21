package org.sjtu.se.ipads.fdbserver.tsdb.log;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogManager {
    private String filePath;
    private List<LogEntry> restoreEntries;
    int fileSize;
    int fileMaxCounter;     // the max number of file
    static int fileCounter;
    private static int FILE_SIZE = 1024 * 1024 * 10;

    public LogManager(int fileMaxCounter, boolean clear) {
        String path = this.getClass().getClassLoader().getResource("").getPath();
        path += "/log";
        this.filePath = path;
        this.fileCounter = 0;
        this.fileMaxCounter = fileMaxCounter;

        if (clear) {
            File file = new File(this.filePath + "/");
            deleteFile(file);
        }
    }

    public LogManager(String filePath, int fileMaxCounter, boolean clear) {
        this.filePath = filePath;
        this.fileCounter = 0;
        this.fileMaxCounter = fileMaxCounter;

        if (clear) {
            File file = new File(this.filePath + "/");
            deleteFile(file);
        }
    }


    public static void deleteFile(File file){
        if (file == null || !file.exists()){
            System.out.println("the file not exist");
            return;
        }

        File[] files = file.listFiles();
        for (File f: files) {
            String name = file.getName();
            System.out.println(name);

            if (f.isDirectory()){
                deleteFile(f);
            }else {
                f.delete();
            }
        }
    }


    public void appendLog(LogEntry entry) {
        Map.Entry<Integer, byte[]> packedEntry = entry.serialize();
        int size = packedEntry.getKey();


        // If the file is too large, write it to next file
        if (fileSize + size > FILE_SIZE) {
            fileCounter = (fileCounter + 1) % fileMaxCounter;
            // clear the new file (if exist)
            try {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath + fileCounter + ".bin"));
                if (bufferedWriter != null) {
                    bufferedWriter.write("");
                    bufferedWriter.flush();
                    bufferedWriter.close();
                }
                fileSize = 0;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // write to the file
        try {
            // assure the log file exist
            File file = new File(filePath + fileCounter + ".bin");
            if (!file.exists()) {
                if (!file.getParentFile().mkdirs()) {
                    System.out.println("create parent directories error");     // Will create parent directories if not exists
                }
                if (!file.createNewFile()) {
                    System.out.println("create new file error");
                }
            }

            OutputStream out = new BufferedOutputStream(new FileOutputStream(filePath + fileCounter + ".bin", true));
            System.out.println("the write file is : " + filePath + fileCounter + ".bin");
            out.write(packedEntry.getValue());
            out.flush();
            out.close();
            fileSize += size;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void restoreLog() {
        try {
            if (restoreEntries != null) {
                restoreEntries.clear();
            } else {
                restoreEntries =  new ArrayList<>();
            }

            for (int i = 0; i < fileMaxCounter; ++i) {
                File file = new File(filePath + i + ".bin");
                if (!file.exists()) {
                    continue;
                }
                FileInputStream in = new FileInputStream(filePath + i + ".bin");
                while (in.available() > 0) {
                    // read the entry size
                    byte[] bytesForSize = new byte[4];
                    if (in.read(bytesForSize) == -1) {
                        break;
                    } else {
                        int size = LogEntry.byteArrayToInt(bytesForSize, 0);
                        byte[] rawData = new byte[size];
                        if (in.read(rawData) != -1) {
                            LogEntry newEntry = new LogEntry();
                            newEntry.deserialize(rawData, size);
                            restoreEntries.add(newEntry);
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public List<LogEntry> getRestoreEntries() {
        return this.restoreEntries;
    }
    // TODO: maybe need snapshot and so on
}
