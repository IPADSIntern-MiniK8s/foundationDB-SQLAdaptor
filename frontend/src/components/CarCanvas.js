import {Button, Input, Layout, Slider, Space, Table, theme, Image, TimePicker, DatePicker} from "antd";
import {Content, Header} from "antd/es/layout/layout";
import {MyMenu} from "../components/MyMenu";
import {useEffect, useRef, useState} from "react";
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
    console.log(dataPoints)
    if(dataPoints.length === 0){
        return
    }
    console.log(car_id)

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

const IMG_WIDTH=400
const SIZE = 650;
export const CarCanvas = (props) => {
    const {
        token: { colorBgContainer },
    } = theme.useToken();

    console.log(props.datas)
    // props.datas[20].X  = props.datas[19].X + 100;
    // props.datas[21].X  = props.datas[20].X + 100;
    // props.datas[22].X  = props.datas[21].X + 100;
    // props.datas[20].Y  = props.datas[19].Y + 100;
    // props.datas[21].Y  = props.datas[20].Y + 100;
    // props.datas[22].Y  = props.datas[21].Y + 100;
    let timestamps = props.datas.map(data=>data.TIME_STAMP);
    let Xs = props.datas.map(data=>data.X);
    let Ys = props.datas.map(data=>data.Y);
    let maxTs = Math.max(...timestamps);
    let minTs = Math.min(...timestamps);
    let maxX = Math.max(...Xs);
    let minX = Math.min(...Xs);
    let maxY = Math.max(...Ys);
    let minY = Math.min(...Ys);

    console.log(maxX,minX,maxY,minY)

    const convertX = x=>{
        if(maxX===minX) {
            return (x - minX) / maxX *  (SIZE-50)+20;
        }
        return (x-minX)/(maxX-minX)* (SIZE-50)+20;
        };
    const convertY = y=>{
        if(maxY===minY) {
            return (y - minY) / maxY * (SIZE-50)+20;
        }
        return (y-minY)/(maxY-minY) * (SIZE-50)+20;
    };

    let carids = {};

    for(let datai of props.datas){
        carids[datai.CAR_ID] = 1;
    }


    const [timeNow,setTimeNow] = useState(maxTs)

    const canvasRef = useRef(null);

    if(canvasRef.current){
        let ctx = canvasRef.current.getContext("2d");
        ctx.clearRect(0,0,canvasRef.current.width,canvasRef.current.height);

        drawLines(ctx,canvasRef.current)
        for(const carid of Object.keys(carids)){
            drawCar(ctx,canvasRef.current,props.datas.filter(data=>data.TIME_STAMP <= timeNow && data.CAR_ID ===carid).map(data=>{return {x:convertX(data.X),y:convertY(data.Y)}}),parseInt(carid))
        }
    }
    const imgs = []
    for(const carid of Object.keys(carids)){
        let data = props.datas.filter(data=>Math.abs(data.TIME_STAMP - timeNow) < 700000000 && data.CAR_ID===carid);
        data.sort();
        if(data.length>0){
            imgs.push(
                <Image width={`${IMG_WIDTH}px`} src={"data:image/png;base64,"+data[0].IMG}/>
            )
        }
    }
    return(
        <Space direction={"vertical"}>
            {props.datas.length>0 && ts2str(timeNow/1000000)}
            <Slider min={minTs} max={maxTs} defaultValue={maxTs} onChange={v=>{
                setTimeNow(v);
            }}/>
            <Space>
                <canvas ref={canvasRef} id="canvas" width={`${SIZE}px`}height={`${SIZE}px`}/>
                <div style={{width:`${IMG_WIDTH}px`}}>

                    <Space direction={"vertical"}>
                        {imgs}
                    </Space>
                </div>
            </Space>
        </Space>
    )
};
