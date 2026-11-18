from os import environ

# LEI-related staging dataset tables
TABLES = [
    {
        'table_id': 'lei2',
        'table_name': 'Entities',
    },
    {
        'table_id': 'rr',
        'table_name': 'Ownership',
    },
    {
        'table_id': 'repex',
        'table_name': 'Exceptions',
    }
]


# LEI-related master dataset tables
MASTER_TABLES = [
    "Entities",
    "Ownership",
    "Exceptions",
]

# ISIN-related staging dataset tables
ISIN_TABLES = [
    {
        'table_name': 'Isins',
    },
]

# ISIN-related master dataset tables
ISIN_MASTER_TABLES = [
    "Isins"
]