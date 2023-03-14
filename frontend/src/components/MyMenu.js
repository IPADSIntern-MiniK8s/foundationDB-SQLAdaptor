import {Link, useLocation, useNavigate} from "react-router-dom";
import {Button, Menu} from "antd";
import React, {useEffect, useState} from "react";

export const MyMenu = (props)=>{
    // console.log(isMobile());
    const location = useLocation()
    // console.log(location.pathname)
    const [key,setKey] = useState([])
    const publicPages = [{path:"/",name:"sql"},{path:"/canvas",name:"可视化"}];
    const [pages,setPages] = useState(publicPages)
    // const navigate = useNavigate();
    useEffect(()=>{
        setKey([location.pathname]);
    },[location])
    return (
        <Menu
            theme="light"
            mode={"horizontal"}
            // onClick={(data)=>console.log(data)}
            // defaultSelectedKeys={[{index}+""]}
            selectedKeys={key}
            styles={{zIndex:5}}
            items={pages.map((data, i) => {
                return {
                    key:data.path,
                    label:
                        (
                            <Link style={{marginLeft:"3px"}}to={data.path}>
                                {props.hideText ? '':'  '+data.name}
                            </Link>
                        )
                };
            })}
        />
    );
}
