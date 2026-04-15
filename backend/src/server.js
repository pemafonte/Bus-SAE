require("dotenv").config();
const express = require("express");
const path = require("path");
const cors = require("cors");
const morgan = require("morgan");
const authRoutes = require("./routes/auth");
const serviceRoutes = require("./routes/services");
const supervisorRoutes = require("./routes/supervisor");
const gtfsRoutes = require("./routes/gtfs");
const viewerRoutes = require("./routes/viewer");

const app = express();

app.use(cors());
app.use(express.json({ limit: "15mb" }));
app.use(morgan("dev"));
const projectFrontend = path.join(__dirname, "..", "..", "frontend");
app.use(
  "/frontend",
  express.static(projectFrontend, {
    setHeaders(res, filePath) {
      if (filePath.endsWith(".js") || filePath.endsWith(".html")) {
        res.setHeader("Cache-Control", "no-store, max-age=0");
      }
    },
  })
);
app.use(
  "/fixtures",
  express.static(path.join(projectFrontend, "fixtures"), {
    setHeaders(res, filePath) {
      if (filePath.endsWith(".csv") || filePath.endsWith(".txt")) {
        res.setHeader("Cache-Control", "no-store, max-age=0");
      }
    },
  })
);

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.get("/map-tiles/:z/:x/:y.png", async (req, res) => {
  const { z, x, y } = req.params;
  const ySafe = String(y || "").replace(/[^0-9]/g, "");
  const primaryUrl = `https://tile.openstreetmap.org/${encodeURIComponent(z)}/${encodeURIComponent(
    x
  )}/${encodeURIComponent(ySafe)}.png`;
  const fallbackUrl = `https://a.basemaps.cartocdn.com/light_all/${encodeURIComponent(z)}/${encodeURIComponent(
    x
  )}/${encodeURIComponent(ySafe)}.png`;

  const fetchTile = async (url) => {
    const response = await fetch(url, {
      headers: {
        "User-Agent": "Cursor-Projetos-Mapa/1.0",
      },
    });
    if (!response.ok) return null;
    const arrayBuffer = await response.arrayBuffer();
    return Buffer.from(arrayBuffer);
  };

  try {
    let tile = await fetchTile(primaryUrl);
    if (!tile) tile = await fetchTile(fallbackUrl);
    if (!tile) {
      return res.status(502).json({ message: "Nao foi possivel obter o tile do mapa." });
    }
    res.setHeader("Content-Type", "image/png");
    res.setHeader("Cache-Control", "public, max-age=300");
    return res.status(200).send(tile);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao obter tile do mapa." });
  }
});

app.use("/auth", authRoutes);
app.use("/services", serviceRoutes);
app.use("/supervisor", supervisorRoutes);
app.use("/gtfs", gtfsRoutes);
app.use("/viewer", viewerRoutes);

app.use((err, _req, res, _next) => {
  console.error(err);
  res.status(500).json({ message: "Erro interno do servidor." });
});

const port = Number(process.env.PORT || 4000);
app.listen(port, () => {
  console.log(`API ativa em http://localhost:${port}`);
});
