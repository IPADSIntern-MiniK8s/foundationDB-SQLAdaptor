import {getRequest, postRequest_formData} from "../utils/Ajax";

const apiUrl = "http://124.70.138.28:8080"

export const queryBySQL = (sql,callback)=>{
    getRequest(apiUrl+"/queryBySQL?SQL="+sql,callback)
}
