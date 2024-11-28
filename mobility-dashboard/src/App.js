import React from "react";
import Dashboard from "./components/Dashboard"; //ignore this error
import IngestDataComponent from "./components/IngestData";

function App() {
  return (
    <div className="App" style={{ marginLeft: "20px" }}>
      <h1>Smart Mobility Dashboard</h1>
      <IngestDataComponent />
      <Dashboard/>
    </div>
  );
}

export default App;

