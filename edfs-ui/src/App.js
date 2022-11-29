import Node from './Node';
import Terminal from './Terminal';
import { useState } from 'react';
import './App.css';

function App() {
  const [apiPrefix, setApiPrefix] = useState("/");
  const [reloadUI, setReloadUI] = useState(false);
  return (
    <div className="App">
      <fieldset className="edfs-radio">
        <legend>Choose an emulation type:</legend>
        <div>
          <input type="radio" name="edfs-type" value="/" onChange={e => setApiPrefix(e.target.value)} checked={apiPrefix === "/"}/>MySQL
        </div>
        <div>
          <input type="radio" name="edfs-type" value="/firebase_" onChange={e => setApiPrefix(e.target.value)} />Firebase
        </div>
      </fieldset>
      <div>
        <label htmlFor="browser">EDFS File Explorer</label>
        <Node id="browser" apiPrefix={apiPrefix} reloadUI={reloadUI} setReloadUI={setReloadUI} />
      </div>
      <div>
        <label htmlFor="terminal">EDFS Command Prompt</label>
        <Terminal apiPrefix={apiPrefix} setReloadUI={setReloadUI} />
      </div>
    </div>
  );
}

export default App;
