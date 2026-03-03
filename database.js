const fs = require("fs");
const path = require("path");
const sqlite3 = require("sqlite3").verbose();

const dataDir = path.join(__dirname, "data");
const dbPath = path.join(dataDir, "food_orders.db");
const READ_ONLY = ["1", "true"].includes(String(process.env.READ_ONLY || "").toLowerCase());

if (!fs.existsSync(dataDir)) {
  if (READ_ONLY) {
    throw new Error("Read-only mode requires the data directory to exist.");
  }
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new sqlite3.Database(
  dbPath,
  READ_ONLY ? sqlite3.OPEN_READONLY : sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE
);
const MAX_QTY_PER_ITEM = 10;

function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) return reject(err);
      resolve({ lastID: this.lastID, changes: this.changes });
    });
  });
}

function get(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) return reject(err);
      resolve(row);
    });
  });
}

function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

const categoriesSeed = [
  "Appetizers",
  "Wings",
  "Drumsticks",
  "Full Leg",
  "Wraps",
  "Sandwiches",
  "Hot Dogs",
  "Extras"
];

const menuSeed = [
  { category: "Wings", name: "BBQ Wings (6 pcs)", price: 150, prep_time_minutes: 18 },
  { category: "Wings", name: "Peri Peri Wings (6 pcs)", price: 150, prep_time_minutes: 18 },
  { category: "Wings", name: "Sweet Chilli Wings (6 pcs)", price: 160, prep_time_minutes: 18 },
  { category: "Wings", name: "Tandoori Wings (6 pcs)", price: 180, prep_time_minutes: 18 },

  { category: "Drumsticks", name: "BBQ Drumsticks (2 pcs)", price: 160, prep_time_minutes: 20 },
  { category: "Drumsticks", name: "Peri Peri Drumsticks (2 pcs)", price: 160, prep_time_minutes: 20 },
  { category: "Drumsticks", name: "Sweet Chilli Drumsticks (2 pcs)", price: 180, prep_time_minutes: 20 },
  { category: "Drumsticks", name: "Tandoori Drumsticks (2 pcs)", price: 200, prep_time_minutes: 20 },

  { category: "Full Leg", name: "BBQ Tangdi Kebab", price: 220, prep_time_minutes: 20 },
  { category: "Full Leg", name: "Peri Peri Tangdi Kebab", price: 220, prep_time_minutes: 20 },
  { category: "Full Leg", name: "Sweet Chilli Tangdi Kebab", price: 240, prep_time_minutes: 20 },
  { category: "Full Leg", name: "Tandoori Tangdi Kebab", price: 260, prep_time_minutes: 20 },

  { category: "Wraps", name: "BBQ Chicken Wrap", price: 130, prep_time_minutes: 8 },
  { category: "Wraps", name: "Cheese BBQ Wrap", price: 160, prep_time_minutes: 8 },
  { category: "Wraps", name: "Peri Peri Chicken Wrap", price: 150, prep_time_minutes: 8 },
  { category: "Wraps", name: "Cheese Peri Peri Wrap", price: 180, prep_time_minutes: 8 },
  { category: "Wraps", name: "Tandoori Chicken Wrap", price: 170, prep_time_minutes: 8 },
  { category: "Wraps", name: "Cheese Tandoori Wrap", price: 200, prep_time_minutes: 8 },
  { category: "Wraps", name: "Chicken Sausage Wrap", price: 150, prep_time_minutes: 10 },
  { category: "Wraps", name: "Cheese Sausage Wrap", price: 180, prep_time_minutes: 10 },

  { category: "Sandwiches", name: "BBQ Chicken Sub Sandwich", price: 130, prep_time_minutes: 8 },
  { category: "Sandwiches", name: "Cheese BBQ Sub Sandwich", price: 160, prep_time_minutes: 8 },
  { category: "Sandwiches", name: "Peri Peri Sub Sandwich", price: 150, prep_time_minutes: 8 },
  { category: "Sandwiches", name: "Cheese Peri Peri Sub Sandwich", price: 180, prep_time_minutes: 8 },
  { category: "Sandwiches", name: "Tandoori Chicken Sub Sandwich", price: 170, prep_time_minutes: 8 },
  { category: "Sandwiches", name: "Cheese Tandoori Sub Sandwich", price: 200, prep_time_minutes: 8 },

  { category: "Hot Dogs", name: "Chicken Hotdog", price: 100, prep_time_minutes: 10 },
  { category: "Hot Dogs", name: "Cheese Chicken Hotdog", price: 130, prep_time_minutes: 10 },
  { category: "Hot Dogs", name: "Peri Peri Hotdog", price: 120, prep_time_minutes: 10 },
  { category: "Hot Dogs", name: "Cheese Peri Peri Hotdog", price: 150, prep_time_minutes: 10 },

  { category: "Extras", name: "Extra Sauce Dip", price: 10, prep_time_minutes: 1 },
  { category: "Extras", name: "Extra Cheese", price: 30, prep_time_minutes: 1 }
];

const appetizerGroupsSeed = [
  {
    name: "BBQ Chicken Breast",
    variants: [
      { portion_name: "Mini", price: 100, prep_time_minutes: 8 },
      { portion_name: "Half", price: 160, prep_time_minutes: 8 },
      { portion_name: "Full", price: 300, prep_time_minutes: 8 }
    ]
  },
  {
    name: "Tandoori Chicken Breast",
    variants: [
      { portion_name: "Mini", price: 120, prep_time_minutes: 8 },
      { portion_name: "Half", price: 180, prep_time_minutes: 8 },
      { portion_name: "Full", price: 340, prep_time_minutes: 8 }
    ]
  },
  {
    name: "Grilled Chicken Sausages",
    variants: [
      { portion_name: "1 pc", price: 60, prep_time_minutes: 10 },
      { portion_name: "2 pcs", price: 100, prep_time_minutes: 10 },
      { portion_name: "3 pcs", price: 150, prep_time_minutes: 10 }
    ]
  }
];

const legacyNameMigrations = [
  { oldName: "BBQ Chicken Sub", newName: "BBQ Chicken Sub Sandwich" },
  { oldName: "Cheese BBQ Sub", newName: "Cheese BBQ Sub Sandwich" },
  { oldName: "Peri Peri Sub", newName: "Peri Peri Sub Sandwich" },
  { oldName: "Cheese Peri Peri Sub", newName: "Cheese Peri Peri Sub Sandwich" },
  { oldName: "Tandoori Chicken Sub", newName: "Tandoori Chicken Sub Sandwich" },
  { oldName: "Cheese Tandoori Sub", newName: "Cheese Tandoori Sub Sandwich" }
];

function toINDateTime(date = new Date()) {
  const options = {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  };
  const formatted = new Intl.DateTimeFormat("en-GB", options).format(date);
  const [d, t] = formatted.split(", ");
  const [day, month, year] = d.split("/");
  return `${year}-${month}-${day} ${t}`;
}

function todayIN() {
  return toINDateTime().slice(0, 10);
}

async function hasColumn(tableName, columnName) {
  const columns = await all(`PRAGMA table_info(${tableName})`);
  return columns.some((c) => c.name === columnName);
}

async function getOrdersSchemaFlags() {
  const tableExists = await get(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'orders'"
  );
  if (!tableExists) {
    return { hasItemsJson: false, hasDateKey: false, hasOrderType: false, hasCustomerName: false };
  }

  const hasItemsJson = await hasColumn("orders", "items_json");
  const hasDateKey = await hasColumn("orders", "date_key");
  const hasOrderType = await hasColumn("orders", "order_type");
  const hasCustomerName = await hasColumn("orders", "customer_name");
  return { hasItemsJson, hasDateKey, hasOrderType, hasCustomerName };
}

async function migrateOrdersTable() {
  const hasOrderType = await hasColumn("orders", "order_type");
  if (!hasOrderType) {
    await run("ALTER TABLE orders ADD COLUMN order_type TEXT NOT NULL DEFAULT 'dine_in'");
  }

  const hasCustomerName = await hasColumn("orders", "customer_name");
  if (!hasCustomerName) {
    await run("ALTER TABLE orders ADD COLUMN customer_name TEXT");
  }
}

async function migrateOrderItemsTable() {
  const exists = await get(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'order_items'"
  );
  if (!exists) return;

  const hasItemType = await hasColumn("order_items", "item_type");
  const hasVariant = await hasColumn("order_items", "appetizer_variant_id");
  if (hasItemType && hasVariant) return;

  await run("BEGIN TRANSACTION");
  try {
    await run("ALTER TABLE order_items RENAME TO order_items_legacy");
    await run(`
      CREATE TABLE order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        item_type TEXT NOT NULL CHECK(item_type IN ('menu_item', 'appetizer')),
        menu_item_id INTEGER,
        appetizer_variant_id INTEGER,
        quantity INTEGER NOT NULL CHECK(quantity > 0),
        line_total REAL NOT NULL CHECK(line_total >= 0),
        FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE,
        FOREIGN KEY(menu_item_id) REFERENCES menu_items(id),
        FOREIGN KEY(appetizer_variant_id) REFERENCES appetizer_variants(id),
        CHECK(
          (item_type = 'menu_item' AND menu_item_id IS NOT NULL AND appetizer_variant_id IS NULL) OR
          (item_type = 'appetizer' AND appetizer_variant_id IS NOT NULL AND menu_item_id IS NULL)
        )
      )
    `);

    await run(`
      INSERT INTO order_items (id, order_id, item_type, menu_item_id, appetizer_variant_id, quantity, line_total)
      SELECT id, order_id, 'menu_item', menu_item_id, NULL, quantity, line_total
      FROM order_items_legacy
    `);

    await run("DROP TABLE order_items_legacy");
    await run("COMMIT");
  } catch (error) {
    await run("ROLLBACK");
    throw error;
  }
}

async function initDatabase() {
  if (READ_ONLY) return;
  await run(`
    CREATE TABLE IF NOT EXISTS categories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS menu_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      category_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      price REAL NOT NULL CHECK(price >= 0),
      prep_time_minutes INTEGER NOT NULL CHECK(prep_time_minutes >= 0),
      FOREIGN KEY(category_id) REFERENCES categories(id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS appetizer_groups (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS appetizer_variants (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      group_id INTEGER NOT NULL,
      portion_name TEXT NOT NULL,
      price REAL NOT NULL CHECK(price >= 0),
      prep_time_minutes INTEGER NOT NULL CHECK(prep_time_minutes >= 0),
      FOREIGN KEY(group_id) REFERENCES appetizer_groups(id) ON DELETE CASCADE
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      token_number INTEGER NOT NULL,
      total_amount REAL NOT NULL CHECK(total_amount >= 0),
      payment_mode TEXT NOT NULL CHECK(payment_mode IN ('cash', 'upi')),
      order_type TEXT NOT NULL DEFAULT 'dine_in',
      customer_name TEXT,
      status TEXT NOT NULL CHECK(status IN ('queued', 'preparing', 'ready', 'completed')),
      created_at TEXT NOT NULL
    )
  `);

  await migrateOrdersTable();
  await run(`
    CREATE TABLE IF NOT EXISTS order_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id INTEGER NOT NULL,
      item_type TEXT NOT NULL CHECK(item_type IN ('menu_item', 'appetizer')),
      menu_item_id INTEGER,
      appetizer_variant_id INTEGER,
      quantity INTEGER NOT NULL CHECK(quantity > 0),
      line_total REAL NOT NULL CHECK(line_total >= 0),
      FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE,
      FOREIGN KEY(menu_item_id) REFERENCES menu_items(id),
      FOREIGN KEY(appetizer_variant_id) REFERENCES appetizer_variants(id),
      CHECK(
        (item_type = 'menu_item' AND menu_item_id IS NOT NULL AND appetizer_variant_id IS NULL) OR
        (item_type = 'appetizer' AND appetizer_variant_id IS NOT NULL AND menu_item_id IS NULL)
      )
    )
  `);

  await migrateOrderItemsTable();

  await run("CREATE INDEX IF NOT EXISTS idx_menu_items_category_id ON menu_items(category_id)");
  await run("CREATE UNIQUE INDEX IF NOT EXISTS idx_menu_items_unique_name_per_category ON menu_items(category_id, name)");
  await run("CREATE INDEX IF NOT EXISTS idx_appetizer_variants_group_id ON appetizer_variants(group_id)");
  await run("CREATE UNIQUE INDEX IF NOT EXISTS idx_appetizer_variants_group_portion ON appetizer_variants(group_id, portion_name)");
  await run("CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at)");
  await run("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)");
  await run("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)");
  await run("CREATE INDEX IF NOT EXISTS idx_order_items_variant_id ON order_items(appetizer_variant_id)");
  await run(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_daily_token_unique
    ON orders(token_number, date(created_at))
  `);

  await run("BEGIN TRANSACTION");
  try {
    const oldHotdogsCategory = await get("SELECT id FROM categories WHERE name = 'Hotdogs'");
    const newHotDogsCategory = await get("SELECT id FROM categories WHERE name = 'Hot Dogs'");
    if (oldHotdogsCategory && !newHotDogsCategory) {
      await run("UPDATE categories SET name = 'Hot Dogs' WHERE id = ?", [oldHotdogsCategory.id]);
    }

    for (const categoryName of categoriesSeed) {
      await run("INSERT OR IGNORE INTO categories (name) VALUES (?)", [categoryName]);
    }

    const oldHotdogsAfterSeed = await get("SELECT id FROM categories WHERE name = 'Hotdogs'");
    const newHotDogsAfterSeed = await get("SELECT id FROM categories WHERE name = 'Hot Dogs'");
    if (oldHotdogsAfterSeed && newHotDogsAfterSeed) {
      await run(
        "UPDATE menu_items SET category_id = ? WHERE category_id = ?",
        [newHotDogsAfterSeed.id, oldHotdogsAfterSeed.id]
      );
    }

    const categoryRows = await all("SELECT id, name FROM categories");
    const categoryMap = new Map(categoryRows.map((row) => [row.name, row.id]));

    for (const migration of legacyNameMigrations) {
      await run("UPDATE menu_items SET name = ? WHERE name = ?", [migration.newName, migration.oldName]);
    }

    for (const item of menuSeed) {
      const existing = await get(
        `
        SELECT mi.id
        FROM menu_items mi
        INNER JOIN categories c ON c.id = mi.category_id
        WHERE c.name = ? AND mi.name = ?
        `,
        [item.category, item.name]
      );

      if (existing) {
        await run(
          `
          UPDATE menu_items
          SET category_id = ?, price = ?, prep_time_minutes = ?
          WHERE id = ?
          `,
          [categoryMap.get(item.category), item.price, item.prep_time_minutes, existing.id]
        );
      } else {
        await run(
          "INSERT INTO menu_items (category_id, name, price, prep_time_minutes) VALUES (?, ?, ?, ?)",
          [categoryMap.get(item.category), item.name, item.price, item.prep_time_minutes]
        );
      }
    }

    for (const groupSeed of appetizerGroupsSeed) {
      await run("INSERT OR IGNORE INTO appetizer_groups (name) VALUES (?)", [groupSeed.name]);
      const groupRow = await get("SELECT id FROM appetizer_groups WHERE name = ?", [groupSeed.name]);
      for (const variant of groupSeed.variants) {
        const existingVariant = await get(
          "SELECT id FROM appetizer_variants WHERE group_id = ? AND portion_name = ?",
          [groupRow.id, variant.portion_name]
        );
        if (existingVariant) {
          await run(
            `
            UPDATE appetizer_variants
            SET price = ?, prep_time_minutes = ?
            WHERE id = ?
            `,
            [variant.price, variant.prep_time_minutes, existingVariant.id]
          );
        } else {
          await run(
            `
            INSERT INTO appetizer_variants (group_id, portion_name, price, prep_time_minutes)
            VALUES (?, ?, ?, ?)
            `,
            [groupRow.id, variant.portion_name, variant.price, variant.prep_time_minutes]
          );
        }
      }
    }

    await run("COMMIT");
  } catch (error) {
    await run("ROLLBACK");
    throw error;
  }
}

async function getMenu() {
  return all(
    `
    SELECT id, name, price, prep_time_minutes, category, type, group_id, variant_id
    FROM (
      SELECT
        CAST(mi.id AS TEXT) AS id,
        mi.name,
        mi.price,
        mi.prep_time_minutes,
        c.name AS category,
        'menu_item' AS type,
        NULL AS group_id,
        NULL AS variant_id,
        c.id AS category_sort,
        mi.id AS item_sort
      FROM menu_items mi
      INNER JOIN categories c ON c.id = mi.category_id
      WHERE c.name != 'Appetizers'

      UNION ALL

      SELECT
        ('a-' || av.id) AS id,
        (ag.name || ' - ' || av.portion_name) AS name,
        av.price,
        av.prep_time_minutes,
        'Appetizers' AS category,
        'appetizer' AS type,
        ag.id AS group_id,
        av.id AS variant_id,
        1 AS category_sort,
        av.id AS item_sort
      FROM appetizer_variants av
      INNER JOIN appetizer_groups ag ON ag.id = av.group_id
    ) t
    ORDER BY t.category_sort ASC, t.item_sort ASC
    `
  );
}

async function getAppetizers() {
  const rows = await all(
    `
    SELECT
      ag.id AS group_id,
      ag.name,
      av.id AS variant_id,
      av.portion_name,
      av.price,
      av.prep_time_minutes
    FROM appetizer_groups ag
    INNER JOIN appetizer_variants av ON av.group_id = ag.id
    ORDER BY ag.id ASC, av.id ASC
    `
  );

  const grouped = new Map();
  for (const row of rows) {
    if (!grouped.has(row.group_id)) {
      grouped.set(row.group_id, {
        group_id: row.group_id,
        name: row.name,
        variants: []
      });
    }
    grouped.get(row.group_id).variants.push({
      id: row.variant_id,
      portion: row.portion_name,
      price: row.price,
      prep_time_minutes: row.prep_time_minutes
    });
  }

  return Array.from(grouped.values());
}

async function fetchOrderRows(includeCompleted = false) {
  let sql = `
    SELECT id, token_number, total_amount, payment_mode, order_type, customer_name, status, created_at
    FROM orders
    WHERE date(created_at) = ?
  `;
  if (!includeCompleted) {
    sql += " AND status != 'completed'";
  }
  sql += " ORDER BY id ASC";
  return all(sql, [todayIN()]);
}

async function attachOrderItems(orderRows) {
  if (orderRows.length === 0) return [];
  const placeholders = orderRows.map(() => "?").join(",");
  const ids = orderRows.map((o) => o.id);

  const items = await all(
    `
    SELECT
      oi.order_id,
      oi.item_type,
      oi.quantity,
      oi.line_total,
      oi.menu_item_id,
      oi.appetizer_variant_id,
      mi.name AS menu_item_name,
      ag.id AS appetizer_group_id,
      ag.name AS appetizer_group_name,
      av.portion_name AS appetizer_portion_name
    FROM order_items oi
    LEFT JOIN menu_items mi ON mi.id = oi.menu_item_id
    LEFT JOIN appetizer_variants av ON av.id = oi.appetizer_variant_id
    LEFT JOIN appetizer_groups ag ON ag.id = av.group_id
    WHERE oi.order_id IN (${placeholders})
    ORDER BY oi.id ASC
    `,
    ids
  );

  const byOrderId = new Map();
  for (const item of items) {
    if (!byOrderId.has(item.order_id)) byOrderId.set(item.order_id, []);

    let displayName = item.menu_item_name;
    if (item.item_type === "appetizer") {
      displayName = `${item.appetizer_group_name} - ${item.appetizer_portion_name}`;
    }

    byOrderId.get(item.order_id).push({
      type: item.item_type,
      menu_item_id: item.menu_item_id,
      group_id: item.appetizer_group_id,
      variant_id: item.appetizer_variant_id,
      name: displayName,
      quantity: item.quantity,
      line_total: item.line_total
    });
  }

  return orderRows.map((order) => ({
    ...order,
    items: byOrderId.get(order.id) || []
  }));
}

async function getOrders(includeCompleted = false) {
  const rows = await fetchOrderRows(includeCompleted);
  return attachOrderItems(rows);
}

async function getOrderById(orderId) {
  const row = await get(
    `
    SELECT id, token_number, total_amount, payment_mode, order_type, customer_name, status, created_at
    FROM orders
    WHERE id = ?
    `,
    [orderId]
  );
  if (!row) return null;
  const [full] = await attachOrderItems([row]);
  return full;
}

async function createOrder({ items, payment_mode, order_type = "dine_in", customer_name = "" }) {
  if (!Array.isArray(items) || items.length === 0) {
    throw new Error("At least one item is required.");
  }
  if (!["cash", "upi"].includes(payment_mode)) {
    throw new Error("Invalid payment mode.");
  }
  if (!["dine_in", "parcel"].includes(order_type)) {
    throw new Error("Invalid order type.");
  }

  const cleanCustomerName = String(customer_name || "").trim().slice(0, 80);

  await run("BEGIN IMMEDIATE TRANSACTION");
  try {
    const tokenRow = await get(
      "SELECT COALESCE(MAX(token_number), 0) + 1 AS next_token FROM orders WHERE date(created_at) = ?",
      [todayIN()]
    );
    const tokenNumber = tokenRow.next_token;

    const menuRows = await all("SELECT id, name, price FROM menu_items");
    const menuMap = new Map(menuRows.map((row) => [row.id, row]));

    const appetizerRows = await all(
      `
      SELECT
        av.id AS variant_id,
        av.group_id,
        av.price,
        ag.name AS group_name,
        av.portion_name
      FROM appetizer_variants av
      INNER JOIN appetizer_groups ag ON ag.id = av.group_id
      `
    );
    const appetizerMap = new Map(appetizerRows.map((row) => [row.variant_id, row]));

    let totalAmount = 0;
    const normalizedItems = [];

    for (const rawItem of items) {
      const type = rawItem.type === "appetizer" ? "appetizer" : "menu_item";
      const quantity = Number(rawItem.quantity);

      if (!Number.isInteger(quantity) || quantity <= 0 || quantity > MAX_QTY_PER_ITEM) {
        throw new Error(`Quantity must be between 1 and ${MAX_QTY_PER_ITEM}.`);
      }

      if (type === "appetizer") {
        const groupId = Number(rawItem.group_id);
        const variantId = Number(rawItem.variant_id);
        if (!Number.isInteger(groupId) || !Number.isInteger(variantId)) {
          throw new Error("Invalid appetizer payload.");
        }

        const variant = appetizerMap.get(variantId);
        if (!variant || variant.group_id !== groupId) {
          throw new Error("Invalid appetizer variant selection.");
        }

        const lineTotal = Number(variant.price) * quantity;
        totalAmount += lineTotal;
        normalizedItems.push({
          item_type: "appetizer",
          menu_item_id: null,
          appetizer_variant_id: variantId,
          quantity,
          line_total: lineTotal
        });
        continue;
      }

      const menuItemId = Number(rawItem.menu_item_id || rawItem.id);
      if (!Number.isInteger(menuItemId)) {
        throw new Error("Invalid menu item payload.");
      }

      const menuItem = menuMap.get(menuItemId);
      if (!menuItem) {
        throw new Error(`Menu item not found: ${menuItemId}`);
      }

      const lineTotal = Number(menuItem.price) * quantity;
      totalAmount += lineTotal;
      normalizedItems.push({
        item_type: "menu_item",
        menu_item_id: menuItemId,
        appetizer_variant_id: null,
        quantity,
        line_total: lineTotal
      });
    }

    const createdAt = toINDateTime();
    const schemaFlags = await getOrdersSchemaFlags();

    const orderColumns = ["token_number", "total_amount", "payment_mode", "status", "created_at"];
    const orderValues = [tokenNumber, totalAmount, payment_mode, "queued", createdAt];

    if (schemaFlags.hasOrderType) {
      orderColumns.push("order_type");
      orderValues.push(order_type);
    }
    if (schemaFlags.hasCustomerName) {
      orderColumns.push("customer_name");
      orderValues.push(cleanCustomerName || null);
    }

    if (schemaFlags.hasItemsJson) {
      orderColumns.push("items_json");
      orderValues.push(JSON.stringify(normalizedItems));
    }
    if (schemaFlags.hasDateKey) {
      orderColumns.push("date_key");
      orderValues.push(todayIN());
    }

    const placeholders = orderColumns.map(() => "?").join(", ");
    const insertOrder = await run(
      `
      INSERT INTO orders (${orderColumns.join(", ")})
      VALUES (${placeholders})
      `,
      orderValues
    );

    for (const item of normalizedItems) {
      await run(
        `
        INSERT INTO order_items (order_id, item_type, menu_item_id, appetizer_variant_id, quantity, line_total)
        VALUES (?, ?, ?, ?, ?, ?)
        `,
        [
          insertOrder.lastID,
          item.item_type,
          item.menu_item_id,
          item.appetizer_variant_id,
          item.quantity,
          item.line_total
        ]
      );
    }

    await run("COMMIT");

    const [createdOrder] = await attachOrderItems([
      {
        id: insertOrder.lastID,
        token_number: tokenNumber,
        total_amount: totalAmount,
        payment_mode,
        order_type,
        customer_name: cleanCustomerName || null,
        status: "queued",
        created_at: createdAt
      }
    ]);
    return createdOrder;
  } catch (error) {
    await run("ROLLBACK");
    throw error;
  }
}

async function updateOrderStatus(orderId, nextStatus) {
  const valid = ["queued", "preparing", "ready", "completed"];
  if (!valid.includes(nextStatus)) throw new Error("Invalid status.");

  const result = await run("UPDATE orders SET status = ? WHERE id = ?", [nextStatus, orderId]);
  if (result.changes === 0) return null;

  const row = await get(
    "SELECT id, token_number, total_amount, payment_mode, order_type, customer_name, status, created_at FROM orders WHERE id = ?",
    [orderId]
  );
  const [full] = await attachOrderItems([row]);
  return full;
}

async function getStats() {
  const row = await get(
    `
    SELECT
      COUNT(*) AS total_orders,
      COALESCE(SUM(CASE WHEN payment_mode = 'cash' THEN total_amount END), 0) AS cash_total,
      COALESCE(SUM(CASE WHEN payment_mode = 'upi' THEN total_amount END), 0) AS upi_total,
      COALESCE(SUM(total_amount), 0) AS grand_total
    FROM orders
    WHERE date(created_at) = ?
    `,
    [todayIN()]
  );

  return {
    total_orders: Number(row.total_orders || 0),
    cash_total: Number(row.cash_total || 0),
    upi_total: Number(row.upi_total || 0),
    grand_total: Number(row.grand_total || 0)
  };
}

async function resetDay() {
  const todaysOrders = await all("SELECT id FROM orders WHERE date(created_at) = ?", [todayIN()]);
  if (todaysOrders.length === 0) return { deleted_orders: 0 };

  const ids = todaysOrders.map((o) => o.id);
  const placeholders = ids.map(() => "?").join(",");

  await run("BEGIN TRANSACTION");
  try {
    await run(`DELETE FROM order_items WHERE order_id IN (${placeholders})`, ids);
    const deletedOrders = await run(`DELETE FROM orders WHERE id IN (${placeholders})`, ids);
    await run("COMMIT");
    return { deleted_orders: deletedOrders.changes };
  } catch (error) {
    await run("ROLLBACK");
    throw error;
  }
}

async function deleteOrder(orderId) {
  const exists = await get("SELECT id FROM orders WHERE id = ?", [orderId]);
  if (!exists) return false;

  await run("BEGIN TRANSACTION");
  try {
    await run("DELETE FROM order_items WHERE order_id = ?", [orderId]);
    await run("DELETE FROM orders WHERE id = ?", [orderId]);
    await run("COMMIT");
    return true;
  } catch (error) {
    await run("ROLLBACK");
    throw error;
  }
}

async function editOrder(orderId, items) {
  if (!Array.isArray(items)) {
    throw new Error("Items payload is required.");
  }

  const existingOrder = await get("SELECT id FROM orders WHERE id = ?", [orderId]);
  if (!existingOrder) {
    return null;
  }

  await run("BEGIN IMMEDIATE TRANSACTION");
  try {
    const menuRows = await all("SELECT id, price FROM menu_items");
    const menuMap = new Map(menuRows.map((row) => [row.id, row]));

    const appetizerRows = await all(
      `
      SELECT
        av.id AS variant_id,
        av.price
      FROM appetizer_variants av
      `
    );
    const appetizerMap = new Map(appetizerRows.map((row) => [row.variant_id, row]));

    const normalizedItems = [];
    let totalAmount = 0;

    for (const rawItem of items) {
      const quantity = Number(rawItem.quantity);
      if (!Number.isInteger(quantity) || quantity < 0 || quantity > MAX_QTY_PER_ITEM) {
        throw new Error(`Quantity must be between 0 and ${MAX_QTY_PER_ITEM}.`);
      }
      if (quantity === 0) continue;

      const itemType =
        rawItem.type === "appetizer" || rawItem.appetizer_variant_id
          ? "appetizer"
          : "menu_item";

      if (itemType === "appetizer") {
        const variantId = Number(rawItem.appetizer_variant_id || rawItem.id);
        if (!Number.isInteger(variantId)) {
          throw new Error("Invalid appetizer item id.");
        }
        const variant = appetizerMap.get(variantId);
        if (!variant) {
          throw new Error(`Appetizer variant not found: ${variantId}`);
        }
        const lineTotal = Number(variant.price) * quantity;
        totalAmount += lineTotal;
        normalizedItems.push({
          item_type: "appetizer",
          menu_item_id: null,
          appetizer_variant_id: variantId,
          quantity,
          line_total: lineTotal
        });
        continue;
      }

      const menuItemId = Number(rawItem.menu_item_id || rawItem.id);
      if (!Number.isInteger(menuItemId)) {
        throw new Error("Invalid menu item id.");
      }
      const menuItem = menuMap.get(menuItemId);
      if (!menuItem) {
        throw new Error(`Menu item not found: ${menuItemId}`);
      }
      const lineTotal = Number(menuItem.price) * quantity;
      totalAmount += lineTotal;
      normalizedItems.push({
        item_type: "menu_item",
        menu_item_id: menuItemId,
        appetizer_variant_id: null,
        quantity,
        line_total: lineTotal
      });
    }

    if (normalizedItems.length === 0) {
      throw new Error("At least one item with quantity above 0 is required.");
    }

    await run("DELETE FROM order_items WHERE order_id = ?", [orderId]);
    for (const item of normalizedItems) {
      await run(
        `
        INSERT INTO order_items (order_id, item_type, menu_item_id, appetizer_variant_id, quantity, line_total)
        VALUES (?, ?, ?, ?, ?, ?)
        `,
        [
          orderId,
          item.item_type,
          item.menu_item_id,
          item.appetizer_variant_id,
          item.quantity,
          item.line_total
        ]
      );
    }

    const schemaFlags = await getOrdersSchemaFlags();
    const updateFragments = ["total_amount = ?"];
    const updateParams = [totalAmount];
    if (schemaFlags.hasItemsJson) {
      updateFragments.push("items_json = ?");
      updateParams.push(JSON.stringify(normalizedItems));
    }
    updateParams.push(orderId);
    await run(`UPDATE orders SET ${updateFragments.join(", ")} WHERE id = ?`, updateParams);

    await run("COMMIT");
    return getOrderById(orderId);
  } catch (error) {
    await run("ROLLBACK");
    throw error;
  }
}

module.exports = {
  initDatabase,
  getMenu,
  getAppetizers,
  getOrders,
  getOrderById,
  createOrder,
  editOrder,
  updateOrderStatus,
  getStats,
  resetDay,
  deleteOrder
};
