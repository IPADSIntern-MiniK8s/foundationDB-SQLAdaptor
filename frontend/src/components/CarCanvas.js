import {Button, Input, Layout, Slider, Space, Table, theme, Image, TimePicker, DatePicker} from "antd";
import {Content, Header} from "antd/es/layout/layout";
import {MyMenu} from "../components/MyMenu";
import {useEffect, useState} from "react";
import {getImagesByTs, queryBySQL} from "../service/dataService";
import {image_mock, image_mock2, time2seconds, ts2str} from "../utils/util";
import dayjs from "dayjs";


const drawLines = (ctx,canvas)=>{
    ctx.strokeStyle = "#eee";

    let step = 20;

    for (let x = step + 0.5; x < canvas.width; x += step) {
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, canvas.height);
        ctx.stroke();
    }

    for (let y = step + 0.5; y < canvas.height; y += step) {
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(canvas.width, y);
        ctx.stroke();
    }
}


const carid2color = car_id=>{
    switch (car_id){
        case 0:
            return "blue";
        case 1:
            return "red";
        case 2:
            return "green";
    }
}
const drawCar = (ctx,canvas,dataPoints,car_id)=>{

    if(dataPoints.length === 0){
        return
    }

    ctx.strokeStyle = carid2color(car_id);
    ctx.lineWidth = 2;

    ctx.beginPath();
    ctx.moveTo(dataPoints[0].x, dataPoints[0].y);

    for (let i = 1; i < dataPoints.length; i++) {
        ctx.lineTo(dataPoints[i].x, dataPoints[i].y);
    }

    ctx.stroke();


    let last = dataPoints[dataPoints.length-1];

    ctx.beginPath();
    ctx.arc(last.x, last.y, 10, 0, 2 * Math.PI, false);
    ctx.fillStyle = carid2color(car_id);
    ctx.fill();

    ctx.closePath();

}
let pointsN = 1000;
let maxTs = 1678782445117078342;
let minTs = 1678782419367315554;
let dataPoints_car0 = [];
for(let i = 0 ;i < pointsN;i++){
    let lambda = i/pointsN;
    dataPoints_car0.push({
        x:i*0.5,
        y:i*0.5,
        t:lambda*maxTs + (1-lambda)*minTs
    })
}

let dataPoints_car1 = [];
for(let i = 0 ;i < pointsN;i++){
    let lambda = i/pointsN;
    dataPoints_car1.push({
        x:650-i*0.5,
        y:i*0.5,
        t:lambda*maxTs + (1-lambda)*minTs
    })
}
export const CarCanvas = (props) => {
    const {
        token: { colorBgContainer },
    } = theme.useToken();

    console.log(props.datas)
    let maxTs = 0;
    let minTs = 0;

    let carids = {};

    if(props.datas.length!==0){
        maxTs = props.datas[0].TIME_STAMP;
        minTs = props.datas[0].TIME_STAMP;
        for(let datai of props.datas){
            if(datai.TIME_STAMP < minTs){
                minTs = datai.TIME_STAMP;
            }
            if(datai.TIME_STAMP > maxTs){
                maxTs = datai.TIME_STAMP;
            }
            carids[datai.CAR_ID] = 1;
        }
    }

    const [timeNow,setTimeNow] = useState(maxTs)
    useEffect(()=>{
        let canvas = document.getElementById("canvas");
        let ctx = canvas.getContext("2d");
        ctx.clearRect(0,0,canvas.width,canvas.height);


        drawLines(ctx,canvas)
        drawCar(ctx,canvas,dataPoints_car0.filter(data=>data.t <= timeNow),0)
        drawCar(ctx,canvas,dataPoints_car1.filter(data=>data.t <= timeNow),1)

    },[timeNow])
    const imgs = []
    for(const carid of Object.keys(carids)){
        let data = props.datas.filter(data=>Math.abs(data.TIME_STAMP - timeNow) < 600000000 && data.CAR_ID===carid);
        if(data.length>0){
            imgs.push(
                <Image width={"400px"} src={"data:image/png;base64,"+data[0].IMG}/>
            )
        }
    }
    return(
        <Space direction={"vertical"}>
            {ts2str(timeNow/1000000)}
            <Slider min={minTs} max={maxTs} defaultValue={maxTs} onChange={v=>{
                setTimeNow(v);
            }}/>
            <Space>
                <canvas id="canvas" width="650px" height="650px"/>
                <Space direction={"vertical"}>
                    {imgs}
                </Space>
            </Space>
        </Space>
    )
};
