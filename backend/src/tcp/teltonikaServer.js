const net = require("net");
const { normalizeImei, decodeCodec8AvlRecords, processTeltonikaEvents } = require("../routes/integrations");

function isTcpDebugEnabled() {
  return String(process.env.TELTONIKA_TCP_DEBUG || "")
    .trim()
    .toLowerCase() === "true";
}

function getRemoteLabel(socket) {
  const ip = String(socket.remoteAddress || "unknown");
  const port = String(socket.remotePort || "?");
  return `${ip}:${port}`;
}

function buildCodec8Events(imei, records) {
  return records.map((record) => ({
    imei,
    lat: record.lat,
    lng: record.lng,
    capturedAt: Number.isFinite(record.timestampMs) ? new Date(record.timestampMs).toISOString() : null,
    speedKmh: record.speedKmh,
    headingDeg: record.angle,
    satellites: record.satellites,
    priority: record.priority,
    altitudeM: record.altitude,
    codec: "codec8",
    source: "tracker",
  }));
}

function createAckBuffer(acceptedRecords) {
  const ack = Buffer.alloc(4);
  ack.writeUInt32BE(Math.max(0, Number(acceptedRecords) || 0), 0);
  return ack;
}

function startTeltonikaTcpServer() {
  const port = Number(process.env.TELTONIKA_TCP_PORT || 0);
  if (!port) return null;
  const debug = isTcpDebugEnabled();
  let connectionSeq = 0;

  const server = net.createServer((socket) => {
    connectionSeq += 1;
    const connectionId = connectionSeq;
    const remoteLabel = getRemoteLabel(socket);
    const state = {
      imei: null,
      buffer: Buffer.alloc(0),
      processing: Promise.resolve(),
    };
    if (debug) console.log(`[teltonika-tcp][c${connectionId}] ligação aberta (${remoteLabel})`);

    const consumeBuffer = async () => {
      while (true) {
        if (!state.imei) {
          if (state.buffer.length < 2) return;
          const imeiLength = state.buffer.readUInt16BE(0);
          if (imeiLength < 10 || imeiLength > 40) {
            if (debug) console.warn(`[teltonika-tcp][c${connectionId}] tamanho IMEI inválido: ${imeiLength}`);
            socket.write(Buffer.from([0x00]));
            socket.destroy();
            return;
          }
          if (state.buffer.length < 2 + imeiLength) return;
          const imeiRaw = state.buffer.subarray(2, 2 + imeiLength).toString("ascii");
          state.buffer = state.buffer.subarray(2 + imeiLength);
          const normalized = normalizeImei(imeiRaw);
          if (!normalized) {
            if (debug) console.warn(`[teltonika-tcp][c${connectionId}] IMEI inválido recebido: "${imeiRaw}"`);
            socket.write(Buffer.from([0x00]));
            socket.destroy();
            return;
          }
          state.imei = normalized;
          if (debug) console.log(`[teltonika-tcp][c${connectionId}] IMEI autenticado: ${state.imei}`);
          socket.write(Buffer.from([0x01]));
          continue;
        }

        if (state.buffer.length < 8) return;
        const preamble = state.buffer.readUInt32BE(0);
        if (preamble !== 0) {
          if (debug) console.warn(`[teltonika-tcp][c${connectionId}] preamble inválido: ${preamble}`);
          socket.write(createAckBuffer(0));
          socket.destroy();
          return;
        }
        const dataFieldLength = state.buffer.readUInt32BE(4);
        const packetLength = 8 + dataFieldLength + 4;
        if (packetLength > 1024 * 1024) {
          if (debug) console.warn(`[teltonika-tcp][c${connectionId}] pacote maior que limite: ${packetLength} bytes`);
          socket.write(createAckBuffer(0));
          socket.destroy();
          return;
        }
        if (state.buffer.length < packetLength) return;

        const packet = state.buffer.subarray(0, packetLength);
        state.buffer = state.buffer.subarray(packetLength);

        try {
          const records = decodeCodec8AvlRecords(packet);
          const events = buildCodec8Events(state.imei, records);
          const result = await processTeltonikaEvents(events);
          if (debug) {
            console.log(
              `[teltonika-tcp][c${connectionId}] imei=${state.imei} records=${records.length} accepted=${result.accepted} rejected=${result.rejectedCount}`
            );
          }
          socket.write(createAckBuffer(result.accepted));
        } catch (error) {
          console.error(
            `[teltonika-tcp][c${connectionId}] erro ao processar pacote (imei=${state.imei || "?"})`,
            error
          );
          socket.write(createAckBuffer(0));
        }
      }
    };

    socket.on("data", (chunk) => {
      state.buffer = Buffer.concat([state.buffer, chunk]);
      state.processing = state.processing
        .then(() => consumeBuffer())
        .catch(() => {
          console.error(`[teltonika-tcp][c${connectionId}] falha no ciclo de consumo.`);
          socket.destroy();
        });
    });

    socket.on("end", () => {
      if (debug) console.log(`[teltonika-tcp][c${connectionId}] ligação encerrada pelo cliente`);
    });

    socket.on("close", () => {
      if (debug) console.log(`[teltonika-tcp][c${connectionId}] ligação fechada`);
    });

    socket.on("error", (error) => {
      console.error(`[teltonika-tcp][c${connectionId}] erro no socket`, error);
    });
  });

  server.listen(port, () => {
    console.log(`Teltonika TCP ativo em 0.0.0.0:${port}`);
    if (debug) console.log("[teltonika-tcp] debug detalhado ativo (TELTONIKA_TCP_DEBUG=true)");
  });

  server.on("error", (error) => {
    console.error("[teltonika-tcp] Erro no servidor TCP.", error);
  });

  return server;
}

module.exports = { startTeltonikaTcpServer };
