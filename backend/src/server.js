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
