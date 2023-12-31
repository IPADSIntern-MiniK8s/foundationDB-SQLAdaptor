import {Button, Input, Layout, Space, Table, theme} from "antd";
import {Content, Header} from "antd/es/layout/layout";
import {MyMenu} from "../components/MyMenu";
import {useState} from "react";
import {queryBySQL} from "../service/dataService";
import {ts2str} from "../utils/util";

const key2title = {
    "TIME_STAMP":"时间戳",
    "X":"x轴坐标",
    "Y":"y轴坐标",
    "V_X":"x轴速度",
    "V_Y":"y轴速度",
    "DIRECTION":"方向",
    "V_R":"转向速度",
    "CAR_ID":"小车编号",
}

export const SQLView = () => {
    const {
        token: { colorBgContainer },
    } = theme.useToken();

    const [sql,setSql] = useState('');
    const [datas,setDatas] = useState([]);

    let columns = [];
    if(datas.length>0){
        let data = datas[0];
        for(const key of Object.keys(data)){
            let title = key2title[key];
            if(title === undefined){
                if(key.indexOf("EXPR")===-1){
                    continue;
                }
                title = key;
            }
            columns.push({
                title,
                dataIndex:key,
                key,
                render:item=><div>{item} {key==="TIME_STAMP"&& ts2str(item/1000000)}</div>
            })
            if(key==="TIME_STAMP"){
                columns[columns.length-1]['sorter'] = (a,b)=>a.TIME_STAMP-b.TIME_STAMP
            }
            if(key==="V_X"){
                columns[columns.length-1]['sorter'] = (a,b)=>a.V_X-b.V_X
            }
            if(key==="V_Y"){
                columns[columns.length-1]['sorter'] = (a,b)=>a.V_Y-b.V_Y
            }
        }
    }
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
                    }}
                >
                    <Input style={{height:"60px",fontSize:"20px",overflow:"scroll"}}placeholder="sql" onChange={e=>setSql(e.target.value)}/>
                    <Button onClick={()=>{queryBySQL(sql,(d)=>{
                        console.log(d[0]);
                        setDatas(d)
                    })}}>查询</Button>
                    <Table dataSource={datas} columns={columns} />
                </div>
            </Content>
        </Layout>);
};
