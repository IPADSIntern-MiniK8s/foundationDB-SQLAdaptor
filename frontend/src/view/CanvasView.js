import {Button, Input, Layout, Slider, Space, Table, theme, Image, TimePicker, DatePicker} from "antd";
import {Content, Header} from "antd/es/layout/layout";
import {MyMenu} from "../components/MyMenu";
import {useEffect, useState} from "react";
import {getImagesByTs, queryBySQL} from "../service/dataService";
import {image_mock, image_mock2, time2seconds, ts2str} from "../utils/util";
import dayjs from "dayjs";
import {CarCanvas} from "../components/CarCanvas";


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
export const CanvasView = () => {
    const {
        token: { colorBgContainer },
    } = theme.useToken();

    const [timeNow,setTimeNow] = useState(maxTs)
    const [selectDate,setSelectDate] = useState(0)
    const [selectTsBegin,setSelectTsBegin] = useState(0)
    const [selectTsEnd,setSelectTsEnd] = useState(0)
    const [datas,setDatas] = useState([])

    return(
        <Layout className="layout">
            <Header style={{backgroundColor:"#ffffff"}}>
                <MyMenu/>
            </Header>
            <Content
                style={{
                    padding: '10px 50px',
                }}
            >
                <div
                    className="site-layout-content"
                    style={{
                        background: colorBgContainer,
                        textAlign:"center"
                    }}
                >
                    <Space direction={"vertical"}>
                        <Space>
                            <Space direction={"vertical"}>
                                <DatePicker placeholder={'日期'} onChange={(time,timeString)=>{
                                    setSelectDate(dayjs(timeString).unix())
                                }}/>
                                <TimePicker.RangePicker  placeholder={['开始','结束']} onChange={
                                    (times,timeStrings)=>{
                                        setSelectTsBegin(time2seconds(timeStrings[0]) + selectDate);
                                        setSelectTsEnd(time2seconds(timeStrings[1])+ selectDate);
                                    }}/>
                                <Button onClick={()=>{
                                    // console.log(selectTsBegin,selectTsEnd);
                                    // console.log(ts2str(selectTsBegin*1000),ts2str(selectTsEnd*1000));
                                    getImagesByTs(selectTsBegin*1000000000,selectTsEnd*1000000000,(data)=>{
                                        setDatas(data.map(
                                            datai=>{return{TIME_STAMP:parseInt(datai.TIME_STAMP),CAR_ID:datai.CAR_ID,
                                            IMG:datai.IMG.substr(2,datai.IMG.length-3)}}
                                        ));
                                    })
                                }}>查询</Button>
                            </Space>
                            <CarCanvas datas={datas}/>
                        </Space>
                    </Space>
                </div>

            </Content>
        </Layout>);
};
