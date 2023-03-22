package org.sjtu.se.ipads.fdbserver.controller;

import org.sjtu.se.ipads.fdbserver.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class QueryController {
    @Autowired
    QueryService queryService;
    @RequestMapping("/getDataByMessageID")
    public String getDataByMessageID(@RequestParam("messageID") int messageID) {
        return queryService.getDataByMessageID(messageID);
    }

    @RequestMapping("/queryBySQL")
    public String queryBySQL(@RequestParam("SQL") String sql) {
        System.out.println(sql);
        String res = queryService.queryBySQL(sql);
        System.out.println(res.length());
        return res;
    }
}
