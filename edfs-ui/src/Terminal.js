import { useEffect, useState } from "react";
import axios from "axios";

const Terminal = ({ apiPrefix }) => {
    let lastInput = null;
    useEffect(() => {
        lastInput.focus();
    });
    useEffect(() => {
        setInputArr([["", ""]]);
    }, [apiPrefix]);

    function getEDFSOutput(command, url) {
        axios({
            method: "GET",
            url: url
        })
            .then((response) => {
                let newResponse = response["data"]["response"];
                if (response["data"]["status"] === "EDFS200") {
                    switch (command[0]) {
                        case "getPartitionLocations":
                        case "getAvgFamilyIncome":
                        case "getAvgTimeInUS":
                            newResponse = JSON.stringify(newResponse, null, 4);
                            break;
                        default:
                            break;
                    }
                }
                let curInput = [...inputArr];
                curInput.push([newResponse, ""]);
                setInputArr(curInput);
            });
    }

    function onEnterPress(event) {
        if (event.keyCode === 13) {
            const command = event.target.value.trim().split(" ").filter(e => e);
            let curInput = [...inputArr];
            if (command.length > 0) {
                switch (command[0]) {
                    case "clear":
                        setInputArr([["", ""]]);
                        break;
                    case "mkdir":
                        if (command.length !== 2) {
                            curInput.push(["Invalid number of arguments for mkdir", ""]);
                            setInputArr(curInput);
                        } else {
                            getEDFSOutput(command, apiPrefix + "mkdir?path=" + command[1]);
                        }
                        break;
                    case "ls":
                        if (command.length !== 2) {
                            curInput.push(["Invalid number of arguments for ls", ""]);
                            setInputArr(curInput);
                        } else {
                            getEDFSOutput(command, apiPrefix + "ls?path=" + command[1]);
                        }
                        break;
                    case "cat":
                        if (command.length !== 2) {
                            curInput.push(["Invalid number of arguments for cat", ""]);
                            setInputArr(curInput);
                        } else {
                            getEDFSOutput(command, apiPrefix + "cat?path=" + command[1]);
                        }
                        break;
                    case "rm":
                        if (command.length !== 2) {
                            curInput.push(["Invalid number of arguments for rm", ""]);
                            setInputArr(curInput);
                        } else {
                            getEDFSOutput(command, apiPrefix + "rm?path=" + command[1]);
                        }
                        break;
                    case "put":
                        if (command.length < 4 || command.length > 5) {
                            curInput.push(["Invalid number of arguments for put", ""]);
                            setInputArr(curInput);
                        } else {
                            let url = apiPrefix + "put?source=" + command[1] + "&destination=" + command[2] + "&partitions=" + command[3];
                            url += command.length === 5 ? "&hash=" + command[4] : "";
                            getEDFSOutput(command, url);
                        }
                        break;
                    case "getPartitionLocations":
                        if (command.length !== 2) {
                            curInput.push(["Invalid number of arguments for getPartitionLocations", ""]);
                            setInputArr(curInput);
                        } else {
                            getEDFSOutput(command, apiPrefix + "getPartitionLocations?path=" + command[1]);
                        }
                        break;
                    case "readPartition":
                        if (command.length !== 3) {
                            curInput.push(["Invalid number of arguments for readPartition", ""]);
                            setInputArr(curInput);
                        } else {
                            getEDFSOutput(command, apiPrefix + "readPartition?path=" + command[1] + "&partition=" + command[2]);
                        }
                        break;
                    case "getAvgFamilyIncome":
                        if (command.length < 2 || command.length > 4) {
                            curInput.push(["Invalid number of arguments for getAvgFamilyIncome", ""]);
                            setInputArr(curInput);
                        } else {
                            let url = "/getAvgFamilyIncome?path=" + command[1];
                            url += command.length >= 3 ? "&debug=" + command[2] : "";
                            url += command.length === 4 ? "&hash=" + command[3] : "";
                            getEDFSOutput(command, url);
                        }
                        break;
                    case "getAvgTimeInUS":
                        if (command.length < 2 || command.length > 4) {
                            curInput.push(["Invalid number of arguments for getAvgTimeInUS", ""]);
                            setInputArr(curInput);
                        } else {
                            let url = "/getAvgTimeInUS?path=" + command[1];
                            url += command.length >= 3 ? "&debug=" + command[2] : "";
                            url += command.length === 4 ? "&hash=" + command[3] : "";
                            getEDFSOutput(command, url);
                        }
                        break;
                    default:
                        curInput.push(["command not found: " + command[command.length - 1], ""]);
                        setInputArr(curInput);
                        break;
                }
                return;
            }
            curInput.push(["", ""]);
            setInputArr(curInput);
        }
        return;
    }

    const [inputArr, setInputArr] = useState([["", ""]]);
    return (
        <div className="edfs-terminal">
            {inputArr.map((arr, id) => {
                if (id === inputArr.length - 1) {
                    return (
                        <div key={id}>
                            {arr[0] &&
                                <pre className="terminal-input">{arr[0]}</pre>
                            }
                            {"edfs-terminal-user:" + (apiPrefix === "/" ? "MySQL" : "Firebase") + " $"}
                            <input
                                key={"input" + id}
                                ref={(input) => { lastInput = input; }}
                                className="terminal-input"
                                onKeyUp={onEnterPress}
                                value={arr[1]}
                                onChange={e => {
                                    const currVal = [...inputArr];
                                    currVal[currVal.length - 1][1] = e.target.value;
                                    setInputArr(currVal);
                                }}
                            />
                        </div>
                    )
                }
                return (
                    <div key={id}>
                        {arr[0] &&
                            <pre className="terminal-input">{arr[0]}</pre>
                        }
                        {"edfs-terminal-user:" + (apiPrefix === "/" ? "MySQL" : "Firebase") + " $"}
                        <input
                            disabled
                            key={"input" + id}
                            className="terminal-input"
                            onKeyUp={onEnterPress}
                            value={arr[1]}
                            onChange={e => {
                                const currVal = [...inputArr];
                                currVal[currVal.length - 1][1] = e.target.value;
                                setInputArr(currVal);
                            }}
                        />
                    </div>
                )
            })}
        </div>
    )
}
export default Terminal;
