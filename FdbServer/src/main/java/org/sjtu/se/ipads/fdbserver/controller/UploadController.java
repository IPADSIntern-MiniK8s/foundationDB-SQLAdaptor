package org.sjtu.se.ipads.fdbserver.controller;


import com.alibaba.fastjson.JSONObject;
import org.sjtu.se.ipads.fdbserver.service.QueryService;
import org.sjtu.se.ipads.fdbserver.service.UploadService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

@RestController
public class UploadController {

//    @Autowired
//    CarService carService;

    @Resource
    UploadService uploadService;

    @PostMapping("/uploadData_v2")
    public boolean uploadData_v2(@RequestBody JSONObject carData) {
        return uploadService.save(carData);
    }

    @PostMapping("/uploadData_multi")
    public int uploadData_multi(@RequestBody List<JSONObject> carDatas) {
        int count = 0;
        for(JSONObject data:carDatas){
            if(uploadService.save(data)){
                count++;
            }
        }
        return count;
    }


}
