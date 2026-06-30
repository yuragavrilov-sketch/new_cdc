import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import { themeCss } from "./theme";

const styleEl = document.createElement("style");
styleEl.textContent = themeCss;
document.head.appendChild(styleEl);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
