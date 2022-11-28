import { useEffect, useState } from "react";
import { ReactComponent as File } from "./file.svg";
import { ReactComponent as Folder } from "./folder.svg";
import { ReactComponent as Back } from "./back.svg";
import axios from "axios";

function getFiles(path, apiPrefix, setContent) {
    axios({
        method: "GET",
        url: apiPrefix+"ls?path=" + (path === "" ? "/" : path)
    })
    .then((response) => {
        const res = response.data.response.split("\n").filter(e => e);
        const newContent = {
            message: res[0],
            items: res.slice(1).map(item => {
                item = item.split("\t").filter(e => e);
                let node_type = item[0][0];
                let relativePath = item[item.length - 1].split("/").filter(e => e);
                let name = relativePath[relativePath.length - 1]
                return {
                    type: node_type === "-" ? "file" : "folder",
                    name: name
                };
            })
        };
        setContent(newContent);
    });
}

const Node = ({apiPrefix}) => {
    const [content, setContent] = useState(null);
    const [filePath, setFilePath] = useState("");
    useEffect(() => {
        setFilePath("");
    }, [apiPrefix])
    useEffect(() => {
        setContent(null);
        getFiles(filePath, apiPrefix, setContent);
    }, [filePath, apiPrefix]);
    return (
        <div className="FileBrowser">
            {filePath && 
                <div onClick={() => {
                    let nodes = filePath.split("/").filter(e => e);
                    nodes.pop();
                    if (nodes.length === 0) {
                        setFilePath("");
                        return;
                    }
                    setFilePath("/"+nodes.join("/"));
                }}>
                    <Back className="Back" />
                </div>
            }
            {
                content?.items.map(item => (
                    <div
                        onDoubleClick={() => {
                            if (item.type === "folder") {
                                setFilePath(filePath+"/"+item.name);
                            }
                        }}
                        key={item.name}
                    >
                        {
                            <div className="Node">
                                {item.type === "file" ? <File /> : <Folder />}
                                <h4 className="Name">{item.name}</h4>
                            </div>
                        }
                    </div>
                ))
            }
        </div>
    )
}

export default Node;