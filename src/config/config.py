class Config:
    DATA_PATH = "/workspaces/ema/data"
    RAW_DATA_PATH = f"{DATA_PATH}/raw"
    BRONZE_DATA_PATH = f"{DATA_PATH}/bronze"
    SILVER_DATA_PATH = f"{DATA_PATH}/silver"
    GOLD_DATA_PATH = f"{DATA_PATH}/gold"
    COLUMNS_TO_BE_RENAMED = {
        "ShipDate": "ShipmentDate",
        "ShipMode": "ShipmentMode",
    }
