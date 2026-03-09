# Data Catalog – Gold Layer

## Overview
The **Gold Layer** represents the business-level data model designed for analytics and reporting.  
It follows a dimensional modeling approach where:

- **Dimension tables** store descriptive attributes.
- **Fact tables** store measurable business events.

This layer is structured using a **Star Schema**, where the fact table is connected to multiple dimensions.

---

# Tables

## 1. gold.dim_customers

**Table Type:** Dimension Table  

**Purpose:**  
Stores enriched customer information including demographic and geographic attributes.  
This table provides descriptive context for sales transactions.

| Column Name | Data Type | Description |
|---|---|---|
| customer_key | INT | Surrogate key uniquely identifying each customer record in the dimension table. |
| customer_id | INT | Unique numerical identifier assigned to each customer. |
| customer_number | NVARCHAR(50) | Alphanumeric identifier representing the customer for business tracking. |
| first_name | NVARCHAR(50) | Customer's first name. |
| last_name | NVARCHAR(50) | Customer's last name or family name. |
| country | NVARCHAR(50) | Country of residence (e.g., Australia). |
| marital_status | NVARCHAR(50) | Marital status of the customer (e.g., Married, Single). |
| gender | NVARCHAR(50) | Gender of the customer (e.g., Male, Female, n/a). |
| birthdate | DATE | Customer’s date of birth. |
| create_date | DATE | Date when the customer record was created. |

---

## 2. gold.dim_products

**Table Type:** Dimension Table  

**Purpose:**  
Contains detailed product information including category, subcategory, and product attributes used for analysis.

| Column Name | Data Type | Description |
|---|---|---|
| product_key | INT | Surrogate key uniquely identifying each product record. |
| product_id | INT | Internal identifier assigned to the product. |
| product_number | NVARCHAR(50) | Business product code used for tracking and inventory. |
| product_name | NVARCHAR(50) | Descriptive product name including key attributes. |
| category_id | NVARCHAR(50) | Identifier representing the product category. |
| category | NVARCHAR(50) | High-level product classification (e.g., Bikes, Components). |
| subcategory | NVARCHAR(50) | More detailed classification of the product. |
| maintenance_required | NVARCHAR(50) | Indicates whether maintenance is required (Yes/No). |
| cost | INT | Base cost of the product. |
| product_line | NVARCHAR(50) | Product line classification (e.g., Road, Mountain). |
| start_date | DATE | Date when the product became available. |

---

## 3. gold.fact_sales

**Table Type:** Fact Table  

**Purpose:**  
Stores transactional sales records. Each row represents a sales event and links to customer and product dimensions.

| Column Name | Data Type | Description |
|---|---|---|
| order_number | NVARCHAR(50) | Unique identifier for the sales order. |
| product_key | INT | Foreign key referencing **dim_products**. |
| customer_key | INT | Foreign key referencing **dim_customers**. |
| order_date | DATE | Date when the order was placed. |
| shipping_date | DATE | Date when the order was shipped. |
| due_date | DATE | Payment due date for the order. |
| sales_amount | INT | Total sales amount for the order line. |
| quantity | INT | Number of units sold. |
| price | INT | Unit price of the product. |

---

# Table Relationships

The **fact_sales** table connects business events to descriptive data stored in dimension tables.

Relationships:

- `fact_sales.customer_key` → `dim_customers.customer_key`
- `fact_sales.product_key` → `dim_products.product_key`

