import {getRequest, postRequest_data, postRequest_formData} from "./utils/Ajax";
import {apiUrl, apiUrlSecret, tokenName} from "./utils/config";
import {history} from "./utils/util";
import {messages} from "./utils/mock";
export const uploadPhoto = (type,photos,callback) => {
    let formData = new FormData();

    for (let file of photos){
        // console.log(file)
        formData.append("photos", file);
    }
    postRequest_formData(
        apiUrlSecret+"/photos/"+type,
        formData,
        callback
    )
}

export const uploadMessage = (pictures,content,callback) => {
    let formData = new FormData();
    // console.log(pictures,content)
    for (let file of pictures){
        // console.log(file)
        formData.append("pictures", file);
    }
    formData.append("content",content);
    formData.append("username",localStorage.getItem(tokenName));
    formData.append("timestamp",new Date().getTime());
    // console.log(formData)
    // console.log(apiUrlSecret)
    postRequest_formData(
        apiUrlSecret+"/message",
        formData,
        callback
    )
}

export const getPhotos = (type,callback) => {
    getRequest(apiUrlSecret+"/photos/"+type,callback)
}
export const getPictures = (callback) => {
    getRequest(apiUrlSecret+"/pictures",callback)
}
export const getMessages = (callback) => {
    getRequest(apiUrlSecret+"/messages",callback)
    // callback({success:true,data:messages})
}

export const getRecentMessages = (callback) => {
    getRequest(apiUrlSecret+"/recentMessages",callback)
    // callback({success:true,data:messages})
}



export const checkSession = (callback,onError) => {
    getRequest(apiUrl+"/auth/checkSession",callback,onError)
    // callback({success:true,data:{username:'szy'}})
}

export const login = (token,callback)=>{
    // console.log(token)
    postRequest_data(apiUrl+"/auth/login",{token},callback)
}
export const logout = (callback)=>{
    postRequest_data(apiUrl+"/auth/logout",{},callback)
}
export const addEmoji = (messageTS,emoji,callback)=>{
    // console.log(messageTs,emoji)
    postRequest_data(apiUrlSecret+"/emoji",{messageTS,emoji},callback)
}


export const sendPicture = (description,dataurl,callback)=>{
    postRequest_formData(apiUrlSecret+"/picture",{description,content:dataurl},callback)
}
