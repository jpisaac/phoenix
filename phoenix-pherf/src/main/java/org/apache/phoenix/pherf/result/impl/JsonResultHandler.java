package org.apache.phoenix.pherf.result.impl;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.phoenix.pherf.result.Result;
import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;

import java.io.File;
import java.util.List;

public class JsonResultHandler extends DefaultResultHandler {

    @Override public void write(Result result) throws Exception {
        File resultFile = new File(resultFileName);
        List<ResultValue> values = result.getResultValues();
        FileUtils.writeStringToFile(resultFile, new Gson().toJson(values));
    }

    @Override public void flush() throws Exception {

    }

    @Override public void close() throws Exception {

    }

    @Override public List<Result> read() throws Exception {
        return null;
    }

    @Override
    public boolean isClosed() {
        return true;
    }

    @Override
    public void setResultFileDetails(ResultFileDetails details) {
        super.setResultFileDetails(ResultFileDetails.JSON);
    }
}
