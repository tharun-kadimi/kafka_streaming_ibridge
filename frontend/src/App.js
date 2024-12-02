import React, { useState, useEffect } from "react";
import "./App.css";

function App() {
  const [message, setMessage] = useState({});

  // Fetch the latest message from the Flask API every 1 second
  useEffect(() => {
    const interval = setInterval(() => {
      fetch("/api/latest_message") // Flask API endpoint
        .then((response) => response.json())
        .then((data) => setMessage(data))
        .catch((err) => console.log("Error fetching data: ", err));
    }, 1000); // Fetch data every second

    return () => clearInterval(interval); // Cleanup the interval on component unmount
  }, []);

  return (
    <div className="App">
      <h1>Latest Kafka Message</h1>

      {/* Table to display Kafka message */}
      <table border="1" cellPadding="10" cellSpacing="0">
        <thead>
          <tr>
            <th>Field</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          {/* Render the message fields in the table */}
          {Object.entries(message).map(([key, value], index) => (
            <tr key={index}>
              <td>{key}</td>
              <td>{JSON.stringify(value)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
