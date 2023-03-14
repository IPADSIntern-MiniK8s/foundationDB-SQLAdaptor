import {message} from "antd";

export const getRequest = (url, callback,onError) => {
    let opts = {
        method: "GET",
        credentials: "include"
    };

    fetch(url,opts)
        .then((response) => {
            return response.json();
        })
        .then((data) => {
            callback(data);
        })
        .catch((error) => {
            if(onError){
                onError();
            }
            console.log(error);
            message.error(error,1);
        });
};

export const postRequest_formData = (url, formData, callback) => {
    let opts = {
        method: "POST",
        body: formData,
        credentials: "include"
    };

    fetch(url,opts)
        .then((response) => {
            return response.json();
        })
        .then((data) => {
            callback(data);
        })
        .catch((error) => {
            console.log(error);
            message.error(error,1);
            // history.push('/login');
            // history.go(0);
        });
};

export const postRequest_data = (url, data, callback) => {
    let formData = new FormData();

    for (let p in data){
        if(data.hasOwnProperty(p))
            formData.append(p, data[p]);
    }

    let opts = {
        method: "POST",
        body: formData,
        credentials: "include"
    };

    fetch(url,opts)
        .then((response) => {
            return response.json();
        })
        .then((data) => {
            callback(data);
        })
        .catch((error) => {
            console.log(error);
            message.error(error,1);
        });
};

export const postRequest_json = (url, json, callback) => {

    let opts = {
        method: "POST",
        body: JSON.stringify(json),
        headers: {
            'Content-Type': 'application/json',
        },
        credentials: "include"
    };

    fetch(url,opts)
        .then((response) => {
            return response.json();
        })
        .then((data) => {
            callback(data);
        })
        .catch((error) => {
            console.log(error);
            message.error(error,1);
        });
};

export const postRequest_json_sync = async (url, json) => {
    let opts = {
        method: "POST",
        body: JSON.stringify(json),
        headers: {
            'Content-Type': 'application/json',
        },
        credentials: "include"
    };

    const response = await fetch(url, opts);
    try {
        return await response.json();
    }catch (e){
        console.log(e);
        message.error(e,1);
        // history.push('/login');
        // history.go(0);
    }
}

export const postRequest_formData_async = async (url, data) => {
    let formData = new FormData();

    for (let p in data){
        if(data.hasOwnProperty(p))
            formData.append(p, data[p]);
    }

    let opts = {
        method: "POST",
        body: formData,
        // credentials: "include"
    };

    const response = await fetch(url, opts);
    try {
        return await response.json();
    }catch (e){
        console.log(e);
        message.error(e,1);
    }
}
