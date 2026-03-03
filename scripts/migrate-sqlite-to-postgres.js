const path = require("path");
const sqlite3 = require("sqlite3").verbose();
const pgDb = require("../database-postgres");

const sqlitePath = path.join(__dirname, "..", "data", "food_orders.db");

function openSqlite() {
  return new sqlite3.Database(sqlitePath, sqlite3.OPEN_READONLY);
}

function sqliteAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

async function ensureEmptyOrAllowed() {
  const existing = await pgDb.query("SELECT COUNT(*)::int AS count FROM categories");
  const count = existing.rows[0]?.count || 0;
  if (count > 0 && !["1", "true"].includes(String(process.env.ALLOW_OVERWRITE || "").toLowerCase())) {
    throw new Error("Postgres already has data. Set ALLOW_OVERWRITE=1 to overwrite.");
  }
  if (count > 0) {
    await pgDb.query("BEGIN");
    try {
      await pgDb.query("TRUNCATE TABLE order_items RESTART IDENTITY CASCADE");
      await pgDb.query("TRUNCATE TABLE orders RESTART IDENTITY CASCADE");
      await pgDb.query("TRUNCATE TABLE appetizer_variants RESTART IDENTITY CASCADE");
      await pgDb.query("TRUNCATE TABLE appetizer_groups RESTART IDENTITY CASCADE");
      await pgDb.query("TRUNCATE TABLE menu_items RESTART IDENTITY CASCADE");
      await pgDb.query("TRUNCATE TABLE categories RESTART IDENTITY CASCADE");
      await pgDb.query("COMMIT");
    } catch (error) {
      await pgDb.query("ROLLBACK");
      throw error;
    }
  }
}

async function insertRows(table, columns, rows) {
  if (rows.length === 0) return;
  for (const row of rows) {
    const values = columns.map((c) => row[c]);
    const placeholders = columns.map((_, i) => `$${i + 1}`).join(", ");
    const sql = `INSERT INTO ${table} (${columns.join(", ")}) VALUES (${placeholders})`;
    await pgDb.query(sql, values);
  }
}

async function resetSequence(table, idColumn = "id") {
  await pgDb.query(
    `SELECT setval(pg_get_serial_sequence('${table}','${idColumn}'), COALESCE((SELECT MAX(${idColumn}) FROM ${table}), 1))`
  );
}

async function migrate() {
  await pgDb.initDatabase();
  await ensureEmptyOrAllowed();

  const sqlite = openSqlite();
  try {
    const categories = await sqliteAll(sqlite, "SELECT id, name FROM categories ORDER BY id");
    const menuItems = await sqliteAll(
      sqlite,
      "SELECT id, category_id, name, price, prep_time_minutes FROM menu_items ORDER BY id"
    );
    const appetizerGroups = await sqliteAll(
      sqlite,
      "SELECT id, name FROM appetizer_groups ORDER BY id"
    );
    const appetizerVariants = await sqliteAll(
      sqlite,
      "SELECT id, group_id, portion_name, price, prep_time_minutes FROM appetizer_variants ORDER BY id"
    );
    const orders = await sqliteAll(
      sqlite,
      "SELECT id, token_number, total_amount, payment_mode, order_type, customer_name, status, created_at FROM orders ORDER BY id"
    );
    const orderItems = await sqliteAll(
      sqlite,
      "SELECT id, order_id, item_type, menu_item_id, appetizer_variant_id, quantity, line_total FROM order_items ORDER BY id"
    );

    await pgDb.query("BEGIN");
    try {
      await insertRows("categories", ["id", "name"], categories);
      await insertRows(
        "menu_items",
        ["id", "category_id", "name", "price", "prep_time_minutes"],
        menuItems
      );
      await insertRows("appetizer_groups", ["id", "name"], appetizerGroups);
      await insertRows(
        "appetizer_variants",
        ["id", "group_id", "portion_name", "price", "prep_time_minutes"],
        appetizerVariants
      );
      await insertRows(
        "orders",
        ["id", "token_number", "total_amount", "payment_mode", "order_type", "customer_name", "status", "created_at"],
        orders
      );
      await insertRows(
        "order_items",
        ["id", "order_id", "item_type", "menu_item_id", "appetizer_variant_id", "quantity", "line_total"],
        orderItems
      );
      await pgDb.query("COMMIT");
    } catch (error) {
      await pgDb.query("ROLLBACK");
      throw error;
    }

    await resetSequence("categories");
    await resetSequence("menu_items");
    await resetSequence("appetizer_groups");
    await resetSequence("appetizer_variants");
    await resetSequence("orders");
    await resetSequence("order_items");
  } finally {
    sqlite.close();
  }
}

migrate()
  .then(() => {
    console.log("Migration complete.");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Migration failed:", error);
    process.exit(1);
  });
