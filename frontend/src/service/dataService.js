import {getRequest, postRequest_formData} from "../utils/Ajax";
import {ts2str} from "../utils/util";

const apiUrl = "http://124.70.138.28:8080"

export const queryBySQL = (sql,callback)=>{
    getRequest(apiUrl+"/queryBySQL?SQL="+sql,callback)
}


export const getImagesByTs = (start,end,callback)=>{
    let sql = `select h.TIME_STAMP,h.CAR_ID,i.IMG from IMAGE  as i join HEADER as h on h.MESSAGE_ID = i.MESSAGE_ID where h.TIME_STAMP >=${start} and h.TIME_STAMP <=${end}`
    getRequest(apiUrl+"/queryBySQL?SQL="+sql,callback)
}
export const getXYImgByTs = (start,end,callback)=>{
    console.log("getXYImgByTs",ts2str(start/1000000),ts2str(end/1000000))
    let sql = `select h.TIME_STAMP,h.CAR_ID,i.IMG,g.X,g.Y from IMAGE  as i join HEADER as h on h.MESSAGE_ID = i.MESSAGE_ID join GEOMETRY as g on h.MESSAGE_ID = g.MESSAGE_ID where h.TIME_STAMP >=${start} and h.TIME_STAMP <=${end}`
    getRequest(apiUrl+"/queryBySQL?SQL="+sql,callback)
}
