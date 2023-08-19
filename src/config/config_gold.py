class ConfigGold:
    SALES_COLUMNS_TO_BE_RENAMED = {
        "ShipDate": "ShipmentDate",
        "ShipMode": "ShipmentMode",
    }
    SALES_COLUMNS_TO_SELECT = [
        "OrderID",
        "OrderDate",
        "ShipmentDate",
        "ShipmentMode",
        "City",
    ]
