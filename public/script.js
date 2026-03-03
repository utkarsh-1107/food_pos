const menuContainer = document.getElementById("menu-container");
const orderForm = document.getElementById("order-form");
const orderTotalEl = document.getElementById("order-total");
const messageEl = document.getElementById("message");
const customerNameInput = document.getElementById("customer-name");
const customerAddressInput = document.getElementById("customer-address");
const orderNotesInput = document.getElementById("order-notes");
const customerDetailsPanel = document.getElementById("customer-details-panel");

const queuedList = document.getElementById("queued-list");
const preparingList = document.getElementById("preparing-list");
const readyList = document.getElementById("ready-list");
const completedList = document.getElementById("completed-list");

const activeTabBtn = document.getElementById("active-tab-btn");
const completedTabBtn = document.getElementById("completed-tab-btn");
const activeBoardView = document.getElementById("active-board-view");
const completedBoardView = document.getElementById("completed-board-view");
const orderSearchInput = document.getElementById("order-search-input");
const filterPayment = document.getElementById("filter-payment");
const filterOrderType = document.getElementById("filter-order-type");

const statOrders = document.getElementById("stat-orders");
const statCash = document.getElementById("stat-cash");
const statUpi = document.getElementById("stat-upi");
const statGrand = document.getElementById("stat-grand");
const resetDayBtn = document.getElementById("reset-day-btn");
const clearCartBtn = document.getElementById("clear-cart-btn");
const dailyCloseReportBtn = document.getElementById("daily-close-report-btn");
const dailyCloseReportOutput = document.getElementById("daily-close-report-output");
const editOrderModal = document.getElementById("edit-order-modal");
const editOrderItemsEl = document.getElementById("edit-order-items");
const editMenuListEl = document.getElementById("edit-menu-list");
const editOrderTotalEl = document.getElementById("edit-order-total");
const saveOrderChangesBtn = document.getElementById("save-order-changes");
const closeEditModalBtn = document.getElementById("close-edit-modal");
const orderPreviewModal = document.getElementById("order-preview-modal");
const orderPreviewBody = document.getElementById("order-preview-body");
const closePreviewModalBtn = document.getElementById("close-preview-modal");

const MAX_QTY_PER_ITEM = 10;
let menuItems = [];
let allOrders = [];
let currentBoardTab = "active";
let cachedAdminPin = "";
let currentEditOrderId = null;
let editCart = [];
let searchTerm = "";
let paymentFilter = "";
let orderTypeFilter = "";
let refreshInProgress = false;
let eventSource = null;
const touchStartXByOrderId = new Map();

const categoryOrder = [
  "Appetizers",
  "Wraps",
  "Wings",
  "Sandwiches",
  "Hotdogs",
  "Hot Dogs",
  "Full Leg",
  "Drumsticks",
  "Extras"
];

function formatCurrency(value) {
  return `Rs ${Number(value).toFixed(2)}`;
}

function formatTime(timestamp) {
  if (!timestamp) return "";

  const raw = String(timestamp);
  const directMatch = raw.match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})/);
  if (directMatch) {
    return `${directMatch[1]} ${directMatch[2]}`;
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return raw;
  }

  const formatted = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).format(parsed);

  const [d, t] = formatted.split(", ");
  const [day, month, year] = d.split("/");
  return `${year}-${month}-${day} ${t}`;
}

function formatOrderType(orderType) {
  if (orderType === "parcel") return "Parcel";
  return "Dine In";
}

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function showMessage(text, type = "success") {
  messageEl.textContent = text;
  messageEl.classList.remove("success", "error");
  if (!text) return;
  messageEl.classList.add(type);
}

async function readJsonOrThrow(response, fallbackMessage) {
  const raw = await response.text();
  let payload = null;
  try {
    payload = raw ? JSON.parse(raw) : {};
  } catch {
    const trimmed = String(raw || "").trim();
    if (trimmed.startsWith("<!DOCTYPE") || trimmed.startsWith("<html")) {
      throw new Error(`Server returned HTML for ${response.url}. Restart server and hard refresh (Ctrl+F5).`);
    }
    throw new Error(trimmed || fallbackMessage);
  }

  if (!response.ok) {
    throw new Error(payload.error || fallbackMessage);
  }

  return payload;
}

function getNextStatus(status) {
  if (status === "queued") return "preparing";
  if (status === "preparing") return "ready";
  if (status === "ready") return "completed";
  return null;
}

function getNextStatusLabel(status) {
  if (status === "queued") return "Move to Preparing";
  if (status === "preparing") return "Move to Ready";
  if (status === "ready") return "Mark Completed";
  return "";
}

function groupByCategory(items) {
  return items.reduce((acc, item) => {
    if (!acc[item.category]) acc[item.category] = [];
    acc[item.category].push(item);
    return acc;
  }, {});
}

function parseAppetizerVariantName(itemName) {
  const splitIndex = itemName.lastIndexOf(" - ");
  if (splitIndex === -1) {
    return { groupName: itemName, portionLabel: "Variant" };
  }
  return {
    groupName: itemName.slice(0, splitIndex),
    portionLabel: itemName.slice(splitIndex + 3)
  };
}

function updateAppetizerRowPrice(rowEl) {
  const checked = rowEl.querySelector(".portion-radio:checked");
  const priceEl = rowEl.querySelector(".appetizer-price");
  if (!checked || !priceEl) return;
  priceEl.textContent = formatCurrency(Number(checked.dataset.price || 0));
}

function createQtyStepper(inputClass, max, dataset = {}) {
  const wrapper = document.createElement("div");
  wrapper.className = "qty-stepper";

  const minusBtn = document.createElement("button");
  minusBtn.type = "button";
  minusBtn.textContent = "-";

  const valueEl = document.createElement("span");
  valueEl.className = "qty-value";
  valueEl.textContent = "0";

  const plusBtn = document.createElement("button");
  plusBtn.type = "button";
  plusBtn.textContent = "+";

  const input = document.createElement("input");
  input.type = "number";
  input.min = "0";
  input.max = String(max);
  input.value = "0";
  input.className = inputClass;
  Object.entries(dataset).forEach(([key, value]) => {
    input.dataset[key] = String(value);
  });
  input.style.display = "none";

  const setValue = (raw) => {
    let value = Number(raw);
    if (!Number.isFinite(value)) value = 0;
    if (value < 0) value = 0;
    if (value > max) value = max;
    input.value = String(value);
    valueEl.textContent = String(value);
    calculateTotal();
  };

  minusBtn.addEventListener("click", () => {
    setValue((Number(input.value) || 0) - 1);
  });
  plusBtn.addEventListener("click", () => {
    setValue((Number(input.value) || 0) + 1);
  });

  wrapper.appendChild(minusBtn);
  wrapper.appendChild(valueEl);
  wrapper.appendChild(plusBtn);
  wrapper.appendChild(input);

  return { wrapper, input, setValue };
}

function renderAppetizerRows(appetizers, sectionEl) {
  const groupedAppetizers = new Map();

  appetizers.forEach((item) => {
    const parsed = parseAppetizerVariantName(item.name);
    if (!groupedAppetizers.has(parsed.groupName)) {
      groupedAppetizers.set(parsed.groupName, []);
    }
    groupedAppetizers.get(parsed.groupName).push({
      ...item,
      groupName: parsed.groupName,
      portionLabel: parsed.portionLabel
    });
  });

  Array.from(groupedAppetizers.entries()).forEach(([groupName, variants], rowIndex) => {
    const row = document.createElement("div");
    row.className = "appetizer-row menu-card";
    row.dataset.groupKey = groupName;

    const main = document.createElement("div");
    main.className = "appetizer-main";

    const title = document.createElement("div");
    title.className = "appetizer-row-title";
    title.textContent = groupName;

    const tabs = document.createElement("div");
    tabs.className = "portion-tabs";
    const radioGroupName = `portion-${rowIndex}-${groupName.replace(/\s+/g, "-").toLowerCase()}`;

    variants.forEach((variant, index) => {
      const tabLabel = document.createElement("label");
      tabLabel.className = "portion-tab";

      const radio = document.createElement("input");
      radio.type = "radio";
      radio.className = "portion-radio";
      radio.name = radioGroupName;
      radio.value = String(variant.variant_id || variant.id);
      radio.dataset.price = String(variant.price);
      radio.dataset.groupId = String(variant.group_id || "");
      if (index === 0) radio.checked = true;
      radio.addEventListener("change", () => {
        updateAppetizerRowPrice(row);
        calculateTotal();
      });

      const text = document.createElement("span");
      text.textContent = variant.portionLabel;

      tabLabel.appendChild(radio);
      tabLabel.appendChild(text);
      tabs.appendChild(tabLabel);
    });

    const price = document.createElement("div");
    price.className = "appetizer-price";
    price.textContent = formatCurrency(variants[0].price);

    const controls = document.createElement("div");
    controls.className = "appetizer-controls";

    const qtyWrap = document.createElement("div");
    qtyWrap.className = "qty-control";
    qtyWrap.innerHTML = "<label>Qty</label>";
    const appetizerStepper = createQtyStepper("appetizer-qty", MAX_QTY_PER_ITEM);
    qtyWrap.appendChild(appetizerStepper.wrapper);

    main.appendChild(title);
    main.appendChild(tabs);
    controls.appendChild(price);
    controls.appendChild(qtyWrap);

    row.appendChild(main);
    row.appendChild(controls);

    sectionEl.appendChild(row);
  });
}

function renderRegularCategory(items, sectionEl) {
  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = "menu-item menu-card";

    const info = document.createElement("div");
    info.className = "menu-item-info";
    info.innerHTML = `<strong>${item.name}</strong><small>${formatCurrency(item.price)} | Prep ${item.prep_time_minutes} mins</small>`;

    const qty = document.createElement("div");
    qty.className = "qty-control";
    qty.innerHTML = "<label>Qty</label>";
    const stepper = createQtyStepper("menu-item-qty", MAX_QTY_PER_ITEM, {
      id: String(item.id),
      price: String(item.price),
      type: item.type || "menu_item",
      ...(item.group_id ? { groupId: String(item.group_id) } : {}),
      ...(item.variant_id ? { variantId: String(item.variant_id) } : {})
    });
    qty.appendChild(stepper.wrapper);
    row.appendChild(info);
    row.appendChild(qty);
    sectionEl.appendChild(row);
  });
}

function renderMenu() {
  const grouped = groupByCategory(menuItems);
  const categories = Object.keys(grouped);
  const knownOrder = categoryOrder.filter((c) => categories.includes(c));
  const unknownOrder = categories.filter((c) => !knownOrder.includes(c));
  const orderedCategories = [...knownOrder, ...unknownOrder];

  menuContainer.innerHTML = "";

  orderedCategories.forEach((category) => {
    const groupEl = document.createElement("section");
    groupEl.className = "menu-group";

    const heading = document.createElement("h3");
    heading.textContent = category;
    groupEl.appendChild(heading);

    if (category === "Appetizers") {
      renderAppetizerRows(grouped[category], groupEl);
    } else {
      renderRegularCategory(grouped[category], groupEl);
    }

    menuContainer.appendChild(groupEl);
  });
}

function calculateTotal() {
  let total = 0;

  const regularInputs = menuContainer.querySelectorAll(".menu-item-qty");
  regularInputs.forEach((input) => {
    const quantity = Number(input.value) || 0;
    const price = Number(input.dataset.price) || 0;
    if (quantity > 0) total += quantity * price;
  });

  const appetizerRows = menuContainer.querySelectorAll(".appetizer-row");
  appetizerRows.forEach((row) => {
    const qtyInput = row.querySelector(".appetizer-qty");
    const checked = row.querySelector(".portion-radio:checked");
    const quantity = Number(qtyInput?.value) || 0;
    const price = Number(checked?.dataset.price) || 0;
    if (quantity > 0) total += quantity * price;
  });

  orderTotalEl.textContent = formatCurrency(total);
  return total;
}

function collectItems() {
  const items = [];

  const regularInputs = menuContainer.querySelectorAll(".menu-item-qty");
  regularInputs.forEach((input) => {
    const quantity = Number(input.value) || 0;
    if (quantity <= 0) return;

    const itemType = input.dataset.type || "menu_item";
    if (itemType === "appetizer") {
      items.push({
        type: "appetizer",
        group_id: Number(input.dataset.groupId),
        variant_id: Number(input.dataset.variantId || input.dataset.id),
        quantity
      });
      return;
    }

    items.push({
      type: "menu_item",
      menu_item_id: Number(input.dataset.id),
      quantity
    });
  });

  const appetizerRows = menuContainer.querySelectorAll(".appetizer-row");
  appetizerRows.forEach((row) => {
    const qtyInput = row.querySelector(".appetizer-qty");
    const checked = row.querySelector(".portion-radio:checked");
    const quantity = Number(qtyInput?.value) || 0;
    if (!checked || quantity <= 0) return;

    items.push({
      type: "appetizer",
      group_id: Number(checked.dataset.groupId),
      variant_id: Number(checked.value),
      quantity
    });
  });

  return items;
}

function clearForm() {
  const inputs = menuContainer.querySelectorAll("input[type='number']");
  inputs.forEach((input) => {
    input.value = "0";
  });

  const radioGroups = new Set();
  const radios = menuContainer.querySelectorAll(".portion-radio");
  radios.forEach((radio) => {
    if (radioGroups.has(radio.name)) return;
    radioGroups.add(radio.name);
    const first = menuContainer.querySelector(`.portion-radio[name="${radio.name}"]`);
    if (first) first.checked = true;
  });

  menuContainer.querySelectorAll(".appetizer-row").forEach((row) => {
    updateAppetizerRowPrice(row);
  });

  orderForm.querySelector("input[name='payment_mode'][value='cash']").checked = true;
  orderForm.querySelector("input[name='order_type'][value='dine_in']").checked = true;
  customerNameInput.value = "";
  if (customerAddressInput) customerAddressInput.value = "";
  if (orderNotesInput) orderNotesInput.value = "";
  if (customerDetailsPanel) customerDetailsPanel.open = false;
  calculateTotal();
}

function renderOrderCard(order) {
  const card = document.createElement("article");
  card.className = `order-card status-${order.status}`;

  const itemsList = order.items.map((item) => `<li>${item.name} x ${item.quantity}</li>`).join("");
  const customerMeta = order.customer_name ? `<p class="order-meta">Customer: ${order.customer_name}</p>` : "";

  card.innerHTML = `
    <p class="order-token">#${order.token_number}</p>
    <ul class="order-items">${itemsList}</ul>
    <p class="order-total">${formatCurrency(order.total_amount)}</p>
    <p class="order-meta">Payment: ${order.payment_mode.toUpperCase()}</p>
    <p class="order-meta">Type: ${formatOrderType(order.order_type)}</p>
    ${customerMeta}
    <p class="order-meta">Time: ${formatTime(order.created_at)}</p>
  `;

  const nextStatus = getNextStatus(order.status);
  const buttonLabel = getNextStatusLabel(order.status);
  const actions = document.createElement("div");
  actions.className = "order-actions";

  if (nextStatus && buttonLabel) {
    const actionBtn = document.createElement("button");
    actionBtn.className = "btn stage-btn";
    actionBtn.type = "button";
    actionBtn.textContent = buttonLabel;
    actionBtn.dataset.action = "status";
    actionBtn.dataset.orderId = String(order.id);
    actionBtn.dataset.nextStatus = nextStatus;
    actions.appendChild(actionBtn);
  }

  if (order.status === "queued") {
    const editBtn = document.createElement("button");
    editBtn.className = "btn edit-order-btn";
    editBtn.type = "button";
    editBtn.textContent = "Edit";
    editBtn.dataset.action = "edit";
    editBtn.dataset.orderId = String(order.id);
    actions.appendChild(editBtn);
  }

  const previewBtn = document.createElement("button");
  previewBtn.className = "btn preview-order-btn";
  previewBtn.type = "button";
  previewBtn.textContent = "Expand";
  previewBtn.dataset.action = "preview";
  previewBtn.dataset.orderId = String(order.id);
  actions.appendChild(previewBtn);

  const deleteBtn = document.createElement("button");
  deleteBtn.className = "btn btn-delete";
  deleteBtn.type = "button";
  deleteBtn.textContent = "Delete Order";
  deleteBtn.dataset.action = "delete";
  deleteBtn.dataset.orderId = String(order.id);
  actions.appendChild(deleteBtn);

  card.appendChild(actions);

  card.addEventListener("touchstart", (event) => {
    const touch = event.changedTouches?.[0];
    if (!touch) return;
    touchStartXByOrderId.set(order.id, touch.clientX);
  });

  card.addEventListener("touchend", (event) => {
    if (window.matchMedia("(min-width: 769px)").matches) return;
    const touch = event.changedTouches?.[0];
    if (!touch) return;
    const startX = touchStartXByOrderId.get(order.id);
    touchStartXByOrderId.delete(order.id);
    if (typeof startX !== "number") return;
    const deltaX = touch.clientX - startX;
    if (Math.abs(deltaX) >= 60) {
      openOrderPreview(order);
    }
  });

  return card;
}

function openOrderPreview(order) {
  if (!orderPreviewModal || !orderPreviewBody) return;
  const customer = order.customer_name ? escapeHtml(order.customer_name) : "Not provided";
  const address = order.customer_address ? escapeHtml(order.customer_address) : "Not provided";
  const notes = order.order_notes ? escapeHtml(order.order_notes) : "Not provided";
  const items = (order.items || [])
    .map((item) => `<li>${escapeHtml(item.name)} x ${item.quantity}</li>`)
    .join("");

  orderPreviewBody.innerHTML = `
    <p><strong>Token:</strong> #${order.token_number}</p>
    <p><strong>Status:</strong> ${escapeHtml(order.status)}</p>
    <p><strong>Payment:</strong> ${escapeHtml(String(order.payment_mode || "").toUpperCase())}</p>
    <p><strong>Order Type:</strong> ${formatOrderType(order.order_type)}</p>
    <p><strong>Total:</strong> ${formatCurrency(order.total_amount)}</p>
    <p><strong>Time:</strong> ${formatTime(order.created_at)}</p>
    <p><strong>Customer:</strong> ${customer}</p>
    <p><strong>Address:</strong> ${address}</p>
    <p><strong>Notes:</strong> ${notes}</p>
    <div>
      <strong>Items:</strong>
      <ul class="order-items">${items}</ul>
    </div>
  `;

  orderPreviewModal.classList.remove("hidden");
  orderPreviewModal.setAttribute("aria-hidden", "false");
}

function closeOrderPreview() {
  if (!orderPreviewModal || !orderPreviewBody) return;
  orderPreviewModal.classList.add("hidden");
  orderPreviewModal.setAttribute("aria-hidden", "true");
  orderPreviewBody.innerHTML = "";
}

async function handleOrderCardAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  const orderId = Number(button.dataset.orderId);
  if (!Number.isInteger(orderId) || orderId <= 0) return;

  try {
    if (action === "preview") {
      const selected = allOrders.find((item) => Number(item.id) === orderId);
      if (selected) {
        openOrderPreview(selected);
      }
      return;
    }
    if (action === "edit") {
      await openEditOrderModal(orderId);
      return;
    }
    if (action === "delete") {
      await deleteOrder(orderId);
      return;
    }
    if (action === "status") {
      const nextStatus = button.dataset.nextStatus;
      if (!nextStatus) return;
      await updateStatus(orderId, nextStatus);
    }
  } catch (error) {
    showMessage(error.message, "error");
  }
}

function closeEditOrderModal() {
  currentEditOrderId = null;
  editCart = [];
  editOrderItemsEl.innerHTML = "";
  if (editMenuListEl) editMenuListEl.innerHTML = "";
  if (editOrderTotalEl) editOrderTotalEl.textContent = formatCurrency(0);
  editOrderModal.classList.add("hidden");
  editOrderModal.setAttribute("aria-hidden", "true");
}

function toEditCartItem(item) {
  const quantity = Number(item.quantity) || 0;
  const unitPrice = quantity > 0 ? Number(item.line_total || 0) / quantity : 0;
  if ((item.type || "menu_item") === "appetizer") {
    return {
      key: `appetizer:${item.variant_id}`,
      type: "appetizer",
      appetizer_variant_id: Number(item.variant_id),
      name: item.name,
      price: unitPrice,
      quantity
    };
  }
  return {
    key: `menu_item:${item.menu_item_id}`,
    type: "menu_item",
    menu_item_id: Number(item.menu_item_id),
    name: item.name,
    price: unitPrice,
    quantity
  };
}

function updateEditTotal() {
  const total = editCart.reduce((sum, item) => sum + Number(item.price) * Number(item.quantity), 0);
  if (editOrderTotalEl) {
    editOrderTotalEl.textContent = formatCurrency(total);
  }
}

function renderEditCart() {
  editOrderItemsEl.innerHTML = "";
  editCart.forEach((item, index) => {
    const row = document.createElement("div");
    row.className = "edit-row";

    const label = document.createElement("span");
    label.className = "edit-row-name";
    label.textContent = item.name;

    const controls = document.createElement("div");
    controls.className = "edit-qty-stepper";
    controls.innerHTML = `
      <button type="button" data-edit-action="decrease" data-index="${index}">-</button>
      <span>${item.quantity}</span>
      <button type="button" data-edit-action="increase" data-index="${index}">+</button>
      <button type="button" class="remove-item-btn" data-edit-action="remove" data-index="${index}">x</button>
    `;

    row.appendChild(label);
    row.appendChild(controls);
    editOrderItemsEl.appendChild(row);
  });
  updateEditTotal();
}

function renderEditMenuList() {
  if (!editMenuListEl) return;
  editMenuListEl.innerHTML = "";
  menuItems.forEach((item) => {
    const row = document.createElement("div");
    row.className = "edit-menu-row";
    row.innerHTML = `
      <span>${item.name} - ${formatCurrency(item.price)}</span>
      <button class="btn add-item-btn" type="button" data-edit-action="add" data-menu-id="${item.id}">Add</button>
    `;
    editMenuListEl.appendChild(row);
  });
}

function findMenuItemById(menuId) {
  return menuItems.find((item) => String(item.id) === String(menuId));
}

function addToEditCart(menuId) {
  const menuItem = findMenuItemById(menuId);
  if (!menuItem) return;

  const type = menuItem.type === "appetizer" ? "appetizer" : "menu_item";
  const key = type === "appetizer" ? `appetizer:${menuItem.variant_id}` : `menu_item:${menuItem.id}`;
  const existing = editCart.find((item) => item.key === key);
  if (existing) {
    existing.quantity = Math.min(MAX_QTY_PER_ITEM, Number(existing.quantity) + 1);
  } else if (type === "appetizer") {
    editCart.push({
      key,
      type,
      appetizer_variant_id: Number(menuItem.variant_id),
      name: menuItem.name,
      price: Number(menuItem.price),
      quantity: 1
    });
  } else {
    editCart.push({
      key,
      type,
      menu_item_id: Number(menuItem.id),
      name: menuItem.name,
      price: Number(menuItem.price),
      quantity: 1
    });
  }
  renderEditCart();
}

function changeEditQty(index, delta) {
  const target = editCart[index];
  if (!target) return;
  target.quantity = Number(target.quantity) + Number(delta);
  if (target.quantity <= 0) {
    editCart.splice(index, 1);
  } else if (target.quantity > MAX_QTY_PER_ITEM) {
    target.quantity = MAX_QTY_PER_ITEM;
  }
  renderEditCart();
}

function handleEditModalAction(event) {
  const actionBtn = event.target.closest("button[data-edit-action]");
  if (!actionBtn) return;
  const action = actionBtn.dataset.editAction;
  if (action === "add") {
    addToEditCart(actionBtn.dataset.menuId);
    return;
  }
  const index = Number(actionBtn.dataset.index);
  if (!Number.isInteger(index)) return;
  if (action === "increase") {
    changeEditQty(index, 1);
    return;
  }
  if (action === "decrease") {
    changeEditQty(index, -1);
    return;
  }
  if (action === "remove") {
    editCart.splice(index, 1);
    renderEditCart();
  }
}

async function openEditOrderModal(orderId) {
  if (!editOrderModal || !editOrderItemsEl) {
    throw new Error("Edit modal is unavailable. Please hard refresh the page.");
  }

  const response = await fetch(`/orders/${orderId}`);
  const raw = await response.text();
  let payload = {};
  try {
    payload = JSON.parse(raw);
  } catch {
    payload = { error: raw };
  }
  if (!response.ok) {
    if (response.status === 404 && String(payload.error || "").includes("Cannot GET")) {
      throw new Error("Edit endpoint unavailable on running server. Please restart server.");
    }
    throw new Error(payload.error || "Failed to load order.");
  }

  currentEditOrderId = orderId;
  editCart = payload.items.map(toEditCartItem);
  renderEditCart();
  renderEditMenuList();
  editOrderModal.classList.remove("hidden");
  editOrderModal.setAttribute("aria-hidden", "false");
}

async function saveOrderChanges() {
  if (!currentEditOrderId) return;
  if (!editOrderItemsEl) {
    throw new Error("Edit modal is unavailable. Please hard refresh the page.");
  }
  if (editCart.length === 0) {
    throw new Error("At least one item is required.");
  }

  const items = editCart.map((entry) => {
    const quantity = Number(entry.quantity) || 0;
    const type = entry.type === "appetizer" ? "appetizer" : "menu_item";
    if (type === "appetizer") {
      return {
        type,
        appetizer_variant_id: Number(entry.appetizer_variant_id),
        quantity
      };
    }
    return {
      type,
      menu_item_id: Number(entry.menu_item_id),
      quantity
    };
  });

  const response = await fetch(`/orders/${currentEditOrderId}/edit`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items })
  });

  const raw = await response.text();
  let payload = {};
  try {
    payload = JSON.parse(raw);
  } catch {
    payload = { error: raw };
  }
  if (!response.ok) {
    if (response.status === 404 && String(payload.error || "").includes("Cannot PUT")) {
      throw new Error("Edit endpoint unavailable on running server. Please restart server.");
    }
    throw new Error(payload.error || "Failed to save order changes.");
  }

  closeEditOrderModal();
  showMessage("Order updated successfully.", "success");
  await Promise.all([fetchOrders(), fetchStats()]);
}

function renderColumn(targetEl, data) {
  targetEl.innerHTML = "";
  if (data.length === 0) {
    const emptyEl = document.createElement("p");
    emptyEl.className = "empty";
    emptyEl.textContent = "No orders";
    targetEl.appendChild(emptyEl);
    return;
  }

  data.forEach((order) => {
    targetEl.appendChild(renderOrderCard(order));
  });
}

function orderMatchesFilters(order) {
  if (paymentFilter && order.payment_mode !== paymentFilter) return false;
  if (orderTypeFilter && order.order_type !== orderTypeFilter) return false;
  if (!searchTerm) return true;

  const tokenText = String(order.token_number || "");
  const customerText = String(order.customer_name || "");
  const itemText = (order.items || []).map((item) => item.name).join(" ");
  const haystack = `${tokenText} ${customerText} ${itemText}`.toLowerCase();
  return haystack.includes(searchTerm);
}

function renderBoards() {
  const visibleOrders = allOrders.filter(orderMatchesFilters);
  const byTokenAsc = (a, b) => Number(a.token_number) - Number(b.token_number);
  const queued = visibleOrders.filter((order) => order.status === "queued").sort(byTokenAsc);
  const preparing = visibleOrders.filter((order) => order.status === "preparing").sort(byTokenAsc);
  const ready = visibleOrders.filter((order) => order.status === "ready").sort(byTokenAsc);
  const completed = visibleOrders.filter((order) => order.status === "completed").sort(byTokenAsc);

  renderColumn(queuedList, queued);
  renderColumn(preparingList, preparing);
  renderColumn(readyList, ready);
  renderColumn(completedList, completed);

  if (currentBoardTab === "completed") {
    activeBoardView.classList.add("hidden");
    completedBoardView.classList.remove("hidden");
    activeTabBtn.classList.remove("active");
    completedTabBtn.classList.add("active");
  } else {
    completedBoardView.classList.add("hidden");
    activeBoardView.classList.remove("hidden");
    completedTabBtn.classList.remove("active");
    activeTabBtn.classList.add("active");
  }
}

function setBoardTab(tab) {
  currentBoardTab = tab;
  renderBoards();
}

async function fetchMenu() {
  const response = await fetch("/menu");
  menuItems = await readJsonOrThrow(response, "Failed to fetch menu.");
  renderMenu();
}

async function fetchOrders() {
  const response = await fetch("/orders?includeCompleted=true");
  allOrders = await readJsonOrThrow(response, "Failed to fetch orders.");
  renderBoards();
}

async function fetchStats() {
  const response = await fetch("/stats");
  const stats = await readJsonOrThrow(response, "Failed to fetch stats.");
  statOrders.textContent = String(stats.total_orders || 0);
  statCash.textContent = formatCurrency(stats.cash_total || 0);
  statUpi.textContent = formatCurrency(stats.upi_total || 0);
  statGrand.textContent = formatCurrency(stats.grand_total || 0);
}

async function createOrder(event) {
  event.preventDefault();
  const items = collectItems();
  if (items.length === 0) {
    showMessage("Please select at least one item.", "error");
    return;
  }

  const overLimitItem = items.find((item) => item.quantity > MAX_QTY_PER_ITEM);
  if (overLimitItem) {
    showMessage(`Maximum ${MAX_QTY_PER_ITEM} quantity allowed per item.`, "error");
    return;
  }

  const paymentMode = orderForm.querySelector("input[name='payment_mode']:checked").value;
  const orderType = orderForm.querySelector("input[name='order_type']:checked").value;
  const customerName = customerNameInput.value.trim();
  const customerAddress = customerAddressInput ? customerAddressInput.value.trim() : "";
  const orderNotes = orderNotesInput ? orderNotesInput.value.trim() : "";

  const response = await fetch("/orders", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      items,
      payment_mode: paymentMode,
      order_type: orderType,
      customer_name: customerName,
      customer_address: customerAddress,
      order_notes: orderNotes
    })
  });

  const payload = await readJsonOrThrow(response, "Failed to create order.");

  showMessage(`Order created successfully. Token #${payload.token_number}`, "success");
  clearForm();
  await Promise.all([fetchOrders(), fetchStats()]);
}

async function updateStatus(orderId, status) {
  const response = await fetch(`/orders/${orderId}/status`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status })
  });

  await readJsonOrThrow(response, "Failed to update status.");
  await Promise.all([fetchOrders(), fetchStats()]);
}

async function resetDay() {
  const confirmed = window.confirm("Reset current day orders? This cannot be undone.");
  if (!confirmed) return;

  const response = await fetch("/reset-day", { method: "POST" });
  const payload = await readJsonOrThrow(response, "Failed to reset day.");

  showMessage(`Day reset complete. Deleted ${payload.deleted_orders} orders.`, "success");
  await Promise.all([fetchOrders(), fetchStats()]);
}

async function deleteOrder(orderId) {
  const confirmed = window.confirm("Delete this order permanently? This action cannot be undone.");
  if (!confirmed) return;

  let pin = cachedAdminPin;
  if (!pin) {
    pin = window.prompt("Enter admin PIN to delete order:");
  }
  if (!pin) return;

  const response = await fetch(`/orders/${orderId}`, {
    method: "DELETE",
    headers: { "x-admin-pin": pin }
  });

  const raw = await response.text();
  let payload = {};
  try {
    payload = JSON.parse(raw);
  } catch {
    payload = { error: raw };
  }

  if (response.status === 404 && String(payload.error || "").includes("Cannot DELETE")) {
    throw new Error("Delete endpoint is unavailable on running server. Please restart server.");
  }

  if (!response.ok) {
    if (response.status === 403) {
      cachedAdminPin = "";
      throw new Error("Invalid admin PIN.");
    }
    throw new Error(payload.error || "Failed to delete order.");
  }

  cachedAdminPin = pin;
  showMessage("Order deleted successfully.", "success");
  await Promise.all([fetchOrders(), fetchStats()]);
}

async function refreshDashboard() {
  if (refreshInProgress) return;
  refreshInProgress = true;
  try {
    await Promise.all([fetchOrders(), fetchStats()]);
  } finally {
    refreshInProgress = false;
  }
}

function formatReport(report) {
  const status = report.by_status || {};
  const orderType = report.by_order_type || {};
  const topItems = report.top_items || [];
  const lines = [
    `Daily Close Report - ${report.date}`,
    "",
    `Total Orders: ${report.summary.total_orders}`,
    `Cash Total: ${formatCurrency(report.summary.cash_total)}`,
    `UPI Total: ${formatCurrency(report.summary.upi_total)}`,
    `Grand Total: ${formatCurrency(report.summary.grand_total)}`,
    "",
    "By Status:",
    `Queued: ${status.queued || 0}`,
    `Preparing: ${status.preparing || 0}`,
    `Ready: ${status.ready || 0}`,
    `Completed: ${status.completed || 0}`,
    "",
    "By Order Type:",
    `Dine In: ${orderType.dine_in || 0}`,
    `Parcel: ${orderType.parcel || 0}`,
    "",
    "Top Items:"
  ];

  if (topItems.length === 0) {
    lines.push("No item sales today.");
  } else {
    topItems.forEach((item, index) => {
      lines.push(`${index + 1}. ${item.name} - ${item.quantity}`);
    });
  }

  return lines.join("\n");
}

async function fetchDailyCloseReport() {
  const response = await fetch("/reports/daily-close");
  const payload = await readJsonOrThrow(response, "Failed to fetch daily close report.");
  if (dailyCloseReportOutput) {
    dailyCloseReportOutput.textContent = formatReport(payload);
  }
}

function connectRealtimeEvents() {
  if (eventSource) return;
  eventSource = new EventSource("/events");
  eventSource.addEventListener("orders_changed", async () => {
    try {
      await refreshDashboard();
    } catch (_error) {
      // polling fallback remains active
    }
  });
  eventSource.onerror = () => {
    if (eventSource) {
      eventSource.close();
      eventSource = null;
    }
    setTimeout(connectRealtimeEvents, 3000);
  };
}

async function verifyBackendHealth() {
  const response = await fetch("/health", { cache: "no-store" });
  await readJsonOrThrow(response, "Backend health check failed.");
}

async function init() {
  try {
    await verifyBackendHealth();
    await fetchMenu();
    await refreshDashboard();
    connectRealtimeEvents();

    orderForm.addEventListener("submit", async (event) => {
      try {
        await createOrder(event);
      } catch (error) {
        showMessage(error.message, "error");
      }
    });

    activeTabBtn.addEventListener("click", () => {
      setBoardTab("active");
    });

    completedTabBtn.addEventListener("click", () => {
      setBoardTab("completed");
    });

    if (orderSearchInput) {
      orderSearchInput.addEventListener("input", (event) => {
        searchTerm = String(event.target.value || "").trim().toLowerCase();
        renderBoards();
      });
    }

    if (filterPayment) {
      filterPayment.addEventListener("change", (event) => {
        paymentFilter = String(event.target.value || "");
        renderBoards();
      });
    }

    if (filterOrderType) {
      filterOrderType.addEventListener("change", (event) => {
        orderTypeFilter = String(event.target.value || "");
        renderBoards();
      });
    }

    resetDayBtn.addEventListener("click", async () => {
      try {
        await resetDay();
      } catch (error) {
        showMessage(error.message, "error");
      }
    });

    clearCartBtn.addEventListener("click", () => {
      clearForm();
      showMessage("Cart cleared.", "success");
    });

    if (dailyCloseReportBtn) {
      dailyCloseReportBtn.addEventListener("click", async () => {
        try {
          await fetchDailyCloseReport();
        } catch (error) {
          showMessage(error.message, "error");
        }
      });
    }

    queuedList.addEventListener("click", handleOrderCardAction);
    preparingList.addEventListener("click", handleOrderCardAction);
    readyList.addEventListener("click", handleOrderCardAction);
    completedList.addEventListener("click", handleOrderCardAction);

    if (closeEditModalBtn && saveOrderChangesBtn && editOrderModal) {
      closeEditModalBtn.addEventListener("click", () => {
        closeEditOrderModal();
      });

      saveOrderChangesBtn.addEventListener("click", async () => {
        try {
          await saveOrderChanges();
        } catch (error) {
          showMessage(error.message, "error");
        }
      });

      editOrderModal.addEventListener("click", (event) => {
        handleEditModalAction(event);
        if (event.target === editOrderModal) {
          closeEditOrderModal();
        }
      });
    }

    if (closePreviewModalBtn) {
      closePreviewModalBtn.addEventListener("click", () => {
        closeOrderPreview();
      });
    }

    if (orderPreviewModal) {
      orderPreviewModal.addEventListener("click", (event) => {
        if (event.target === orderPreviewModal) {
          closeOrderPreview();
        }
      });
    }

    setInterval(async () => {
      try {
        await refreshDashboard();
      } catch (error) {
        showMessage(error.message, "error");
      }
    }, 5000);
  } catch (error) {
    showMessage(error.message, "error");
  }
}

init();
