import { StatTile } from "./ui";
import { t } from "../theme";

interface CdcEvent {
  operation: string; schema: string; table: string;
}

interface Props {
  events: CdcEvent[];
}

export function Stats({ events }: Props) {
  const counts = events.reduce<Record<string, number>>((acc, e) => {
    acc[e.operation] = (acc[e.operation] ?? 0) + 1;
    return acc;
  }, {});

  const tableSet = new Set(events.map((e) => `${e.schema}.${e.table}`));

  const tiles = [
    { label: "Total",  value: events.length,        color: t.text.primary },
    { label: "INSERT", value: counts.INSERT ?? 0,   color: t.green.base   },
    { label: "UPDATE", value: counts.UPDATE ?? 0,   color: t.blue.base    },
    { label: "DELETE", value: counts.DELETE ?? 0,   color: t.red.base     },
    { label: "Tables", value: tableSet.size,        color: t.purple.fg    },
  ];

  return (
    <div style={{ display: "flex", gap: t.space[3], flexWrap: "wrap" }}>
      {tiles.map((tile) => (
        <div key={tile.label} style={{ flex: "1 1 100px", minWidth: 80 }}>
          <StatTile label={tile.label} value={tile.value} color={tile.color} />
        </div>
      ))}
    </div>
  );
}
