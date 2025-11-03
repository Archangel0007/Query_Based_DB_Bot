DROP TABLE IF EXISTS `DimPromotion` CASCADE;
DROP TABLE IF EXISTS `DimShippingLocation` CASCADE;
DROP TABLE IF EXISTS `DimOrderStatus` CASCADE;
DROP TABLE IF EXISTS `DimProduct` CASCADE;
DROP TABLE IF EXISTS `DimCustomer` CASCADE;
DROP TABLE IF EXISTS `FactOrderLineItem` CASCADE;
CREATE TABLE `DimCustomer` (
        `customer_id` INTEGER PRIMARY KEY,
        `customer_name` VARCHAR(255),
        `customer_email` VARCHAR(255),
        `customer_phone` VARCHAR(255)
    );
CREATE TABLE `DimProduct` (
        `product_id` INTEGER PRIMARY KEY,
        `product_name` VARCHAR(255),
        `product_category` VARCHAR(255),
        `product_price` DECIMAL(10, 2)
    );
CREATE TABLE `DimOrderStatus` (
        `order_status` VARCHAR(255),
        `payment_status` VARCHAR(255),
        PRIMARY KEY (`order_status`, `payment_status`)
    );
CREATE TABLE `DimShippingLocation` (
        `shipping_address` VARCHAR(255),
        `shipping_city` VARCHAR(255),
        `shipping_state` VARCHAR(255),
        `shipping_zip` INTEGER,
        PRIMARY KEY (`shipping_address`, `shipping_city`, `shipping_state`, `shipping_zip`)
    );
CREATE TABLE `DimPromotion` (
        `promo_code` VARCHAR(255) PRIMARY KEY
    );
CREATE TABLE `FactOrderLineItem` (
        `order_id` INTEGER,
        `product_id` INTEGER,
        `customer_id` INTEGER,
        `shipping_address` VARCHAR(255),
        `shipping_city` VARCHAR(255),
        `shipping_state` VARCHAR(255),
        `shipping_zip` INTEGER,
        `order_status` VARCHAR(255),
        `payment_status` VARCHAR(255),
        `promo_code` VARCHAR(255),
        `order_date` TIMESTAMP,
        `quantity` INTEGER,
        `total_price` DECIMAL(10, 2),
        `discount` DECIMAL(10, 2),
        `sales_tax` DECIMAL(10, 2),
        PRIMARY KEY (`order_id`, `product_id`),
        FOREIGN KEY (`customer_id`) REFERENCES `DimCustomer`(`customer_id`),
        FOREIGN KEY (`product_id`) REFERENCES `DimProduct`(`product_id`),
        FOREIGN KEY (`order_status`, `payment_status`) REFERENCES `DimOrderStatus`(`order_status`, `payment_status`),
        FOREIGN KEY (`shipping_address`, `shipping_city`, `shipping_state`, `shipping_zip`) REFERENCES `DimShippingLocation`(`shipping_address`, `shipping_city`, `shipping_state`, `shipping_zip`),
        FOREIGN KEY (`promo_code`) REFERENCES `DimPromotion`(`promo_code`)
    );