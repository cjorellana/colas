CREATE TABLE queue_items (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    data TEXT NOT NULL,
    status ENUM('unread', 'read') NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_by VARCHAR(255)
);