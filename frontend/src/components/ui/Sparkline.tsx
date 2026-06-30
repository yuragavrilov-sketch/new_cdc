import React from "react";

interface Props {
  data:    number[];
  width?:  number;
  height?: number;
  color?:  string;
}

export function Sparkline({ data, width = 88, height = 24, color = "currentColor" }: Props) {
  if (!data || data.length < 2) return null;
  const max = Math.max(...data, 1);
  const min = Math.min(...data, 0);
  const range = max - min || 1;
  const stepX = width / (data.length - 1);
  const points = data.map((v, i) => {
    const x = i * stepX;
    const y = height - ((v - min) / range) * (height - 2) - 1;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const last  = data[data.length - 1];
  const lastX = (data.length - 1) * stepX;
  const lastY = height - ((last - min) / range) * (height - 2) - 1;
  return (
    <svg width={width} height={height} style={{ display: "block", color }}>
      <polyline points={points} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round"/>
      <circle cx={lastX} cy={lastY} r={2} fill={color}/>
    </svg>
  );
}
