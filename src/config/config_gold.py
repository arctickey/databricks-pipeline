class ConfigGold:
    ORDERS_COLUMNS_TO_SELECT = [
        "OrderID",
        "OrderDate",
        "ShipmentDate",
        "ShipmentMode",
        "City",
    ]
    CUSTOMER_DAYS_AGO_TO_CALCULATE_ORDERS = [5, 10, 15]

    CUSTOMER_COLUMNS_TO_SELECT = [
        "CustomerID",
        "CustomerFirstName",
        "CustomerLastName",
        "Segment",
        "Country",
        "TotalQuantityOfOrders",
    ] + [
        f"QuantityOfOrdersLast{days}Days"
        for days in CUSTOMER_DAYS_AGO_TO_CALCULATE_ORDERS
    ]
