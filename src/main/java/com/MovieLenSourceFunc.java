package com;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MovieLenSourceFunc implements SourceFunction<String> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        readFromFile(ctx, "/Users/ruoyudai/Documents/workspace/ml-100k/ua.base", 0);
    }

    private void readFromFile(SourceContext<String> ctx, String fileName, int streamId) throws IOException {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(fileName));
            String line = null;
            while (isRunning && (line = br.readLine()) != null) {
                ctx.collect(streamId + "\t" + line);
            }
        } finally {
            if(br != null){
                br.close();
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}

