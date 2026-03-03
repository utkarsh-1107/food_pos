const assert = require("node:assert/strict");

const baseUrl = (process.env.BASE_URL || "http://localhost:3000").trim().replace(/\/+$/, "");

async function parseBody(response) {
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function request(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, options);
  const body = await parseBody(response);
  return { response, body };
}

async function run() {
  console.log(`Running smoke test against ${baseUrl}`);

  const health = await request("/health");
  assert.equal(
    health.response.status,
    200,
    `GET /health expected 200, got ${health.response.status}: ${JSON.stringify(health.body)}`
  );
  assert.equal(health.body.status, "ok", `GET /health unexpected payload: ${JSON.stringify(health.body)}`);

  const menu = await request("/menu");
  assert.equal(
    menu.response.status,
    200,
    `GET /menu expected 200, got ${menu.response.status}: ${JSON.stringify(menu.body)}`
  );
  assert.ok(Array.isArray(menu.body), `GET /menu expected array, got: ${JSON.stringify(menu.body)}`);
  assert.ok(menu.body.length > 0, "GET /menu returned empty array.");

  const firstMenuItem = menu.body.find((item) => item.type === "menu_item");
  assert.ok(firstMenuItem, "No menu_item found in /menu response.");

  const createOrderPayload = {
    items: [{ type: "menu_item", menu_item_id: Number(firstMenuItem.id), quantity: 1 }],
    payment_mode: "cash",
    order_type: "dine_in",
    customer_name: "Smoke Test"
  };

  const createOrder = await request("/orders", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(createOrderPayload)
  });

  assert.equal(
    createOrder.response.status,
    201,
    `POST /orders expected 201, got ${createOrder.response.status}: ${JSON.stringify(createOrder.body)}`
  );
  assert.ok(createOrder.body && createOrder.body.id, `POST /orders missing id: ${JSON.stringify(createOrder.body)}`);

  console.log("Smoke test passed.");
}

run().catch((error) => {
  console.error("Smoke test failed:", error.message || error);
  process.exit(1);
});
