package org.sjtu.se.ipads.fdbserver.controller;


import com.alibaba.fastjson.JSONObject;
import org.sjtu.se.ipads.fdbserver.service.CarService;
import org.sjtu.se.ipads.fdbserver.service.UploadService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

@RestController
public class Controller {

//    @Autowired
//    CarService carService;

    @Resource
    UploadService uploadService;

//    @RequestMapping("/uploadData")
//    public boolean uploadData(
//            @RequestParam("time_stamp") long time_stamp,
//            @RequestParam("car_id") int car_id,
//            @RequestParam("x") double x,
//            @RequestParam("y") double y,
//            @RequestParam("r") double r,
//            @RequestParam("vx") double vx,
//            @RequestParam("vy") double vy,
//            @RequestParam("vr") double vr,
//            @RequestParam("image") String img
//
//    ){
//        return carService.uploadData(time_stamp,car_id,x,y,r,vx,vy,vr,img);
//    }

    @PostMapping("/uploadData_v2")
    public boolean uploadData_v2(@RequestBody JSONObject carData) {
        return uploadService.save(carData);
    }

    @RequestMapping("/getDataByMessageID")
    public String getDataByMessageID(@RequestParam("messageID") int messageID) {
        return uploadService.query(messageID);
    }

}
