package org.sjtu.se.ipads.fdbserver.controller;


import org.sjtu.se.ipads.fdbserver.service.CarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

    @Autowired
    CarService carService;
    @RequestMapping("/uploadData")
    public boolean uploadData(
            @RequestParam("time_stamp") long time_stamp,
            @RequestParam("car_id") int car_id,
            @RequestParam("x") double x,
            @RequestParam("y") double y,
            @RequestParam("r") double r,
            @RequestParam("vx") double vx,
            @RequestParam("vy") double vy,
            @RequestParam("rr") double vr,
            @RequestParam("image") String img

    ){
        return carService.uploadData(time_stamp,car_id,x,y,r,vx,vy,vr,img);
    }

}
