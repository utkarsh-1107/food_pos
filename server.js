const path = require("path");
const express = require("express");
const dbClient = String(process.env.DB_CLIENT || "sqlite").toLowerCase();
const usePostgres = dbClient === "postgres";
const db = usePostgres ? require("./database-postgres") : require("./database");

const app = express();
const PORT = process.env.PORT || 3000;
const IS_VERCEL = Boolean(process.env.VERCEL);
const SKIP_DB_INIT = ["1", "true"].includes(String(process.env.SKIP_DB_INIT || "").toLowerCase());
const READ_ONLY = ["1", "true"].includes(String(process.env.READ_ONLY || "").toLowerCase());
const ADMIN_PIN = process.env.ADMIN_PIN || "1234";
const sseClients = new Set();
const runtimeCache = {
  menu: null,
  menuExpiresAt: 0,
  stats: null,
  statsExpiresAt: 0
};

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

let initError = null;
const initPromise = (SKIP_DB_INIT ? Promise.resolve() : db.initDatabase()).catch((error) => {
  initError = error;
  console.error("Failed to initialize database:", error);
});

async function ensureDatabaseReady(res) {
  await initPromise;
  if (!initError) return true;
  res.status(500).json({
    error: "Database initialization failed.",
    detail: initError.message || String(initError)
  });
  return false;
}

function broadcastEvent(type, payload = {}) {
  const message = `event: ${type}\ndata: ${JSON.stringify({ type, ...payload, ts: Date.now() })}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(message);
    } catch (_error) {
      sseClients.delete(client);
    }
  }
}

function invalidateRuntimeCache() {
  runtimeCache.menu = null;
  runtimeCache.menuExpiresAt = 0;
  runtimeCache.stats = null;
  runtimeCache.statsExpiresAt = 0;
}

app.get("/health", async (req, res) => {
  await initPromise;
  if (initError) {
    return res.status(500).json({
      status: "error",
      error: "Database initialization failed.",
      detail: initError.message || String(initError),
      config: {
        db_client: dbClient,
        is_vercel: IS_VERCEL,
        read_only: READ_ONLY,
        skip_db_init: SKIP_DB_INIT
      }
    });
  }

  return res.json({
    status: "ok",
    database: "ready",
    config: {
      db_client: dbClient,
      is_vercel: IS_VERCEL,
      read_only: READ_ONLY,
      skip_db_init: SKIP_DB_INIT
    }
  });
});

app.get("/events", async (req, res) => {
  if (!(await ensureDatabaseReady(res))) return;
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();
  res.write("retry: 3000\n\n");

  sseClients.add(res);
  req.on("close", () => {
    sseClients.delete(res);
    res.end();
  });
});

app.get("/menu", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    const now = Date.now();
    if (runtimeCache.menu && runtimeCache.menuExpiresAt > now) {
      res.setHeader("Cache-Control", "public, max-age=300, s-maxage=300, stale-while-revalidate=600");
      return res.json(runtimeCache.menu);
    }

    const menu = await db.getMenu();
    runtimeCache.menu = menu;
    runtimeCache.menuExpiresAt = now + 5 * 60 * 1000;
    res.setHeader("Cache-Control", "public, max-age=300, s-maxage=300, stale-while-revalidate=600");
    res.json(menu);
  } catch (error) {
    console.error("GET /menu failed:", error);
    res.status(500).json({
      error: "Failed to fetch menu.",
      detail: error.message || String(error)
    });
  }
});

app.get("/appetizers", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    const appetizers = await db.getAppetizers();
    res.json(appetizers);
  } catch (error) {
    console.error("GET /appetizers failed:", error);
    res.status(500).json({ error: "Failed to fetch appetizers." });
  }
});

app.get("/orders", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    const includeCompleted = req.query.includeCompleted === "true";
    const orders = await db.getOrders(includeCompleted);
    res.setHeader("Cache-Control", "no-store");
    res.json(orders);
  } catch (error) {
    console.error("GET /orders failed:", error);
    res.status(500).json({ error: "Failed to fetch orders." });
  }
});

app.get("/orders/:id", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    const orderId = Number(req.params.id);
    if (!Number.isInteger(orderId) || orderId <= 0) {
      return res.status(400).json({ error: "Invalid order id." });
    }

    const order = await db.getOrderById(orderId);
    if (!order) {
      return res.status(404).json({ error: "Order not found." });
    }
    return res.json(order);
  } catch (error) {
    return res.status(500).json({ error: "Failed to fetch order." });
  }
});

app.post("/orders", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    if (READ_ONLY) {
      return res.status(405).json({ error: "Read-only mode is enabled." });
    }
    const { items, payment_mode, order_type, customer_name, customer_address, order_notes } = req.body;
    const order = await db.createOrder({
      items,
      payment_mode,
      order_type,
      customer_name,
      customer_address,
      order_notes
    });
    invalidateRuntimeCache();
    broadcastEvent("orders_changed", { action: "created", order_id: order.id });
    res.status(201).json(order);
  } catch (error) {
    res.status(400).json({ error: error.message || "Failed to create order." });
  }
});

app.put("/orders/:id/status", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    if (READ_ONLY) {
      return res.status(405).json({ error: "Read-only mode is enabled." });
    }
    const orderId = Number(req.params.id);
    if (!Number.isInteger(orderId) || orderId <= 0) {
      return res.status(400).json({ error: "Invalid order id." });
    }

    const { status } = req.body;
    const updated = await db.updateOrderStatus(orderId, status);
    if (!updated) {
      return res.status(404).json({ error: "Order not found." });
    }

    invalidateRuntimeCache();
    broadcastEvent("orders_changed", { action: "status_updated", order_id: orderId, status });
    return res.json(updated);
  } catch (error) {
    return res.status(400).json({ error: error.message || "Failed to update order status." });
  }
});

app.put("/orders/:id/edit", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    if (READ_ONLY) {
      return res.status(405).json({ error: "Read-only mode is enabled." });
    }

    const orderId = Number(req.params.id);
    if (!Number.isInteger(orderId) || orderId <= 0) {
      return res.status(400).json({ error: "Invalid order id." });
    }

    const { items } = req.body;
    const updated = await db.editOrder(orderId, items);
    if (!updated) {
      return res.status(404).json({ error: "Order not found." });
    }

    invalidateRuntimeCache();
    broadcastEvent("orders_changed", { action: "edited", order_id: orderId });
    return res.json(updated);
  } catch (error) {
    return res.status(400).json({ error: error.message || "Failed to edit order." });
  }
});

app.delete("/orders/:id", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    if (READ_ONLY) {
      return res.status(405).json({ error: "Read-only mode is enabled." });
    }
    const orderId = Number(req.params.id);
    if (!Number.isInteger(orderId) || orderId <= 0) {
      return res.status(400).json({ error: "Invalid order id." });
    }

    const providedPin = String(req.header("x-admin-pin") || req.body?.admin_pin || "").trim();
    if (!providedPin || providedPin !== String(ADMIN_PIN).trim()) {
      return res.status(403).json({ error: "Admin access denied." });
    }

    const deleted = await db.deleteOrder(orderId);
    if (!deleted) {
      return res.status(404).json({ error: "Order not found." });
    }

    invalidateRuntimeCache();
    broadcastEvent("orders_changed", { action: "deleted", order_id: orderId });
    return res.json({ message: "Order deleted." });
  } catch (error) {
    return res.status(400).json({ error: error.message || "Failed to delete order." });
  }
});

app.get("/stats", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    const now = Date.now();
    if (runtimeCache.stats && runtimeCache.statsExpiresAt > now) {
      res.setHeader("Cache-Control", "no-store");
      return res.json(runtimeCache.stats);
    }

    const stats = await db.getStats();
    runtimeCache.stats = stats;
    runtimeCache.statsExpiresAt = now + 2 * 1000;
    res.setHeader("Cache-Control", "no-store");
    res.json(stats);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch stats." });
  }
});

app.get("/reports/daily-close", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    const report = await db.getDailyCloseReport();
    res.json(report);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch daily close report." });
  }
});

app.post("/reset-day", async (req, res) => {
  try {
    if (!(await ensureDatabaseReady(res))) return;
    if (READ_ONLY) {
      return res.status(405).json({ error: "Read-only mode is enabled." });
    }
    const result = await db.resetDay();
    invalidateRuntimeCache();
    broadcastEvent("orders_changed", { action: "reset_day" });
    res.json({ message: "Day reset complete.", ...result });
  } catch (error) {
    res.status(500).json({ error: "Failed to reset day." });
  }
});

if (!IS_VERCEL) {
  initPromise
    .then(() => {
      if (initError) {
        process.exit(1);
      }
      app.listen(PORT, () => {
        console.log(`Food order system running at http://localhost:${PORT}`);
      });
    })
    .catch(() => process.exit(1));
}

module.exports = app;
