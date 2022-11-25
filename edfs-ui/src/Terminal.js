import { useEffect, useState } from "react";

const Terminal = () => {
    let lastInput = null;
    useEffect(() => {
        lastInput.focus();
    });

    function onEnterPress(event) {
        if (event.keyCode === 13) {
            if (event.target.value === "clear") {
                setInputArr([[directory, ""]]);
                return;
            }
            const curInput = [...inputArr];
            curInput.push([directory, ""]);
            setInputArr(curInput);
        }
        return;
    }
    
    const [directory, setDirectory] = useState("/");
    const [inputArr, setInputArr] = useState([[directory, ""]]);
    return (
        <div className="edfs-terminal">
            {inputArr.map((arr, id) => {
                if (id === inputArr.length - 1) {
                    return (
                        <div key={id}>
                            {"edfs-terminal-user" + arr[0] + " $"}
                            <input
                                key={"input"+id}
                                ref={(input) => {lastInput = input;}}
                                className="terminal-input"
                                onKeyUp={onEnterPress}
                                value={arr[1]}
                                onChange={e => {
                                    const currVal = [...inputArr];
                                    currVal[currVal.length-1][1] = e.target.value;
                                    setInputArr(currVal);
                                }}
                            />
                        </div>
                    )
                }
                return (
                    <div key={id}>
                        {"edfs-terminal-user" + arr[0] + " $"}
                        <input
                            key={"input"+id}
                            className="terminal-input"
                            onKeyUp={onEnterPress}
                            value={arr[1]}
                            onChange={e => {
                                const currVal = [...inputArr];
                                currVal[currVal.length-1][1] = e.target.value;
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