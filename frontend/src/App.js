import './App.css';
import {SQLView} from "./view/SQLView";
import {BrowserRouter as Router, Route, Routes} from 'react-router-dom';
import {CanvasView} from "./view/CanvasView";
function App() {
  return (
      <Router>
        <Routes>
          <Route path="/" element={<SQLView/>}></Route>
          <Route path="/canvas" element={<CanvasView/>}></Route>
        </Routes>
      </Router>
  );
}

export default App;
