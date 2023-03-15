import {Button, Input, Layout, Space, Table, theme} from "antd";
import {Content, Header} from "antd/es/layout/layout";
import {MyMenu} from "../components/MyMenu";
import {useState} from "react";
import {queryBySQL} from "../service/dataService";

const key2title = {
    "TIME_STAMP":"时间戳",
    "X":"x轴坐标",
    "Y":"y轴坐标",
    "V_X":"x轴速度",
    "V_Y":"y轴速度",
    "DIRECTION":"转向速度",
    "R":"方向",
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
            })
            if(key==="TIME_STAMP"){
                columns[columns.length-1]['sorter'] = (a,b)=>a.TIME_STAMP-b.TIME_STAMP
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
                    <Input style={{height:"100px",fontSize:"40px"}}placeholder="sql" onChange={e=>setSql(e.target.value)}/>
                    <Button onClick={()=>{queryBySQL(sql,(d)=>{
                        console.log(d[0]);
                        setDatas(d)
                    })}}>查询</Button>
                    <Table dataSource={datas} columns={columns} />
                </div>
            </Content>
        </Layout>);
};
