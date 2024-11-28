import React from "react";

const DelayList = ({ delays }) => {
  if (!delays.length) return <p>No delays reported.</p>;

  return ( // Output all delays caused by weather
    <ul>
      {delays.map((delay, index) => (
        <li key={index}>
          Bus <strong>{delay.bus_id}</strong> delayed due to{" "}
          <strong>{delay.delay_reason}</strong> at{" "}
          <strong>{delay.timestamp}</strong>.
        </li>
      ))}
    </ul>
  );
};

export default DelayList;
