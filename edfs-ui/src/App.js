import Node from './Node';
import Terminal from './Terminal';
import './App.css';

function App() {
  return (
    <div className="App">
      <div>
        <label htmlFor="browser">EDFS File Explorer</label>
        <Node id="browser" />
      </div>
      <div>
        <label htmlFor="terminal">EDFS Command Prompt</label>
        <Terminal />
      </div>
    </div>
  );
}

export default App;
