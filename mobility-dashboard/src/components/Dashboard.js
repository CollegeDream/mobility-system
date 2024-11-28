import React, { useState } from "react";
import DelayList from "./DelayList";
import VanList from "./VanList";
import { fetchDashboardData, processData } from "../api";

const Dashboard = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleProcessData = async () => {
    setLoading(true);
    setError(null);
    try {
      await processData(); // Trigger endpoint to process ingested data 
      const dashboardData = await fetchDashboardData(); // Fetch updated data
      setData(dashboardData);
    } catch (err) {
      setError("Failed to process or fetch data.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <button onClick={handleProcessData} disabled={loading}>
        {loading ? "Processing..." : "Process and Load Data"}
      </button>
      {error && <p style={{ color: "red" }}>{error}</p>}
      {data && (
        <>
          <h2>Bus Delays</h2>
          <DelayList delays={data.delays} />
          <h2>Van Requirements</h2>
          <VanList vans={data.van_requirements} />
        </>
      )}
    </div>
  );
};

export default Dashboard;
