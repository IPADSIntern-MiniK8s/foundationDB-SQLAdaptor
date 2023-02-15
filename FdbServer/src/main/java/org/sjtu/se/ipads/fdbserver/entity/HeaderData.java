package org.sjtu.se.ipads.fdbserver.entity;

import lombok.Data;

@Data
public class HeaderData {
    private String message_id;
    private String car_id;
    private String timestamp;

    HeaderData(String m_id, String c_id, String time) {
        message_id = m_id;
        car_id = c_id;
        timestamp = time;
    }

    HeaderData() {}
}
