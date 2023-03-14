
const pad = (n)=>{
    if(n < 10){
        return "0"+n
    }
    return ""+n
}
export const ts2str = (ts)=>{
    let d = new Date(ts);
    let dateStr = (d.getFullYear()) + "-" +
        pad(d.getMonth() + 1) + "-" +
        pad(d.getDate()) + " " +
        pad(d.getHours()) + ":" +
        pad(d.getMinutes()) + ":" +
        pad(d.getSeconds());
    return dateStr
}
