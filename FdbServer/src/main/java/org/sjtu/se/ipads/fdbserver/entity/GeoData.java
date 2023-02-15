package org.sjtu.se.ipads.fdbserver.entity;

import lombok.Data;

@Data
public class GeoData {
    private String message_id;
    private String x;
    private String y;
    private String v_x;
    private String v_y;
    private String v_r;
    private String direction;

    GeoData(String message_id, String x, String y, String v_x, String v_y, String v_r, String direction) {
        this.message_id = message_id;
        this.x = x;
        this.y = y;
        this.v_x = v_x;
        this.v_y = v_y;
        this.direction = direction;
    }

    GeoData(){}
}
