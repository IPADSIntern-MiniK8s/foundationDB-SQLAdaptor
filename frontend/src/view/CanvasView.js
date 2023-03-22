import {Button, Input, Layout, Slider, Space, Table, theme, Image, TimePicker, DatePicker} from "antd";
import {Content, Header} from "antd/es/layout/layout";
import {MyMenu} from "../components/MyMenu";
import {useEffect, useState} from "react";
import {getImagesByTs, getXYImgByTs, queryBySQL} from "../service/dataService";
import {image_mock, image_mock2, time2seconds, ts2str} from "../utils/util";
import dayjs from "dayjs";
import {CarCanvas} from "../components/CarCanvas";


export const CanvasView = () => {
    const {
        token: { colorBgContainer },
    } = theme.useToken();

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
                                    getXYImgByTs(selectTsBegin*1000000000,selectTsEnd*1000000000,(data)=>{
                                        console.log(data.length)
                                        setDatas(data.map(
                                            datai=>{return{TIME_STAMP:parseInt(datai.TIME_STAMP),CAR_ID:datai.CAR_ID,
                                            IMG:datai.IMG.substr(2,datai.IMG.length-3),
                                            X:parseFloat(datai.X),Y:parseFloat(datai.Y)
                                            }}
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
