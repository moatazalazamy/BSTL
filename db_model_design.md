```mermaid
erDiagram
    CUSTOMER {
        int customer_id PK
        string first_name
        string last_name
        string email
        string phone_number
        string address
    }

    ORDER {
        int order_id PK
        date order_date
        int customer_id FK
        decimal total_amount
        string status
    }

    PRODUCT {
        int product_id PK
        string product_name
        string description
        decimal price
        int stock_quantity
    }

    ORDER_ITEM {
        int order_item_id PK
        int order_id FK
        int product_id FK
        int quantity
        decimal price_at_purchase
    }

    PAYMENT {
        int payment_id PK
        int order_id FK
        date payment_date
        decimal amount
        string payment_method
    }

    SHIPMENT {
        int shipment_id PK
        int order_id FK
        date shipment_date
        string tracking_number
        string carrier
    }

    CUSTOMER ||--o{ ORDER : places
    ORDER ||--|{ ORDER_ITEM : contains
    PRODUCT ||--o{ ORDER_ITEM : part_of
    ORDER ||--o| PAYMENT : has
    ORDER ||--o| SHIPMENT : ships
