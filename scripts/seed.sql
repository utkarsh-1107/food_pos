BEGIN;
TRUNCATE TABLE order_items, orders, appetizer_variants, appetizer_groups, menu_items, categories RESTART IDENTITY CASCADE;
INSERT INTO categories (id, name) VALUES
(1, 'Appetizers'),
(2, 'Wings'),
(3, 'Drumsticks'),
(4, 'Full Leg'),
(5, 'Wraps'),
(6, 'Sandwiches'),
(7, 'Hot Dogs'),
(8, 'Extras');
INSERT INTO menu_items (id, category_id, name, price, prep_time_minutes) VALUES
(1, 1, 'BBQ Chicken Breast - Mini', 100, 10),
(2, 1, 'BBQ Chicken Breast - Half', 160, 10),
(3, 1, 'BBQ Chicken Breast - Full', 300, 10),
(4, 1, 'Tandoori Chicken Breast - Mini', 129, 10),
(5, 1, 'Tandoori Chicken Breast - Half', 180, 10),
(6, 1, 'Tandoori Chicken Breast - Full', 340, 10),
(7, 1, 'Grilled Chicken Sausage (1 pc)', 60, 10),
(8, 1, 'Grilled Chicken Sausage (2 pcs)', 100, 10),
(9, 1, 'Grilled Chicken Sausage (3 pcs)', 150, 10),
(10, 2, 'BBQ Wings (6 pcs)', 150, 20),
(11, 2, 'Peri Peri Wings (6 pcs)', 150, 20),
(12, 2, 'Sweet Chilli Wings (6 pcs)', 160, 20),
(13, 2, 'Tandoori Wings (6 pcs)', 180, 20),
(14, 3, 'BBQ Drumsticks (2 pcs)', 160, 20),
(15, 3, 'Peri Peri Drumsticks (2 pcs)', 160, 20),
(16, 3, 'Sweet Chilli Drumsticks (2 pcs)', 180, 20),
(17, 3, 'Tandoori Drumsticks (2 pcs)', 200, 20),
(18, 4, 'BBQ Tangdi Kebab', 220, 20),
(19, 4, 'Peri Peri Tangdi Kebab', 220, 20),
(20, 4, 'Sweet Chilli Tangdi Kebab', 240, 20),
(21, 4, 'Tandoori Tangdi Kebab', 260, 20),
(22, 5, 'BBQ Chicken Wrap', 130, 10),
(23, 5, 'Cheese BBQ Wrap', 160, 10),
(24, 5, 'Peri Peri Chicken Wrap', 150, 10),
(25, 5, 'Cheese Peri Peri Wrap', 180, 10),
(26, 5, 'Tandoori Chicken Wrap', 170, 10),
(27, 5, 'Cheese Tandoori Wrap', 200, 10),
(28, 5, 'Chicken Sausage Wrap', 150, 10),
(29, 5, 'Cheese Sausage Wrap', 180, 10),
(30, 6, 'BBQ Chicken Sub Sandwich', 130, 10),
(31, 6, 'Cheese BBQ Sub Sandwich', 160, 10),
(32, 6, 'Peri Peri Sub Sandwich', 150, 10),
(33, 6, 'Cheese Peri Peri Sub Sandwich', 180, 10),
(34, 6, 'Tandoori Chicken Sub Sandwich', 170, 10),
(35, 6, 'Cheese Tandoori Sub Sandwich', 200, 10),
(36, 7, 'Chicken Hotdog', 100, 10),
(37, 7, 'Cheese Chicken Hotdog', 130, 10),
(38, 7, 'Peri Peri Hotdog', 120, 10),
(39, 7, 'Cheese Peri Peri Hotdog', 150, 10),
(40, 8, 'Extra Sauce Dip', 10, 1),
(41, 8, 'Extra Cheese', 30, 1);
INSERT INTO appetizer_groups (id, name) VALUES
(1, 'BBQ Chicken Breast'),
(2, 'Tandoori Chicken Breast'),
(31, 'Grilled Chicken Sausages');
INSERT INTO appetizer_variants (id, group_id, portion_name, price, prep_time_minutes) VALUES
(1, 1, 'Mini', 100, 10),
(2, 1, 'Half', 160, 10),
(3, 1, 'Full', 300, 10),
(4, 2, 'Mini', 120, 10),
(5, 2, 'Half', 180, 10),
(6, 2, 'Full', 340, 10),
(7, 31, '1 pc', 60, 10),
(8, 31, '2 pcs', 100, 10),
(9, 31, '3 pcs', 150, 10);
SELECT setval(pg_get_serial_sequence('categories','id'), COALESCE((SELECT MAX(id) FROM categories), 1));
SELECT setval(pg_get_serial_sequence('menu_items','id'), COALESCE((SELECT MAX(id) FROM menu_items), 1));
SELECT setval(pg_get_serial_sequence('appetizer_groups','id'), COALESCE((SELECT MAX(id) FROM appetizer_groups), 1));
SELECT setval(pg_get_serial_sequence('appetizer_variants','id'), COALESCE((SELECT MAX(id) FROM appetizer_variants), 1));
SELECT setval(pg_get_serial_sequence('orders','id'), COALESCE((SELECT MAX(id) FROM orders), 1));
SELECT setval(pg_get_serial_sequence('order_items','id'), COALESCE((SELECT MAX(id) FROM order_items), 1));
COMMIT;

