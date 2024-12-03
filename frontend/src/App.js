import React, { useState, useEffect } from "react";
import "./App.css";

function App() {
  const [messages, setMessages] = useState([]);

  // Fetch messages from the Flask API every second
  useEffect(() => {
    const interval = setInterval(() => {
      fetch("/api/messages")
        .then((response) => response.json())
        .then((data) => setMessages(data))
        .catch((err) => console.log("Error fetching data: ", err));
    }, 1000);

    return () => clearInterval(interval); // Cleanup interval on component unmount
  }, []);

  return (
    <div className="App">
      <h1>Kafka Messages</h1>
      <table border="1" cellPadding="10" cellSpacing="0">
        <thead>
          <tr>
            <th>ID</th>
            <th>Recorded Date</th>
            <th>User ID</th>
            <th>Minutes</th>
            <th>Inserted At</th>
            <th>Updated At</th>
          </tr>
        </thead>
        <tbody>
          {messages.map((message, index) => (
            <tr key={index}>
              <td>{message.id || "N/A"}</td>
              <td>{message.recorded_date || "N/A"}</td>
              <td>{message.user_id || "N/A"}</td>
              <td>{message.minutes || "N/A"}</td>
              <td>{message.inserted_at || "N/A"}</td>
              <td>{message.updated_at || "N/A"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
