package org.sjtu.se.ipads.fdbserver.entity;

import lombok.Data;

@Data
public class ImgData {
    private String message_id;
    private String img;

    ImgData(String m_id, String img) {
        this.message_id = m_id;
        this.img = img;
    }

    ImgData() {}
}
