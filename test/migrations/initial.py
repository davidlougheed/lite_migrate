from lite_migrate.steps import SQLStep

up = [
    SQLStep("CREATE TABLE testing (pk INTEGER PRIMARY KEY, some_field TEXT)"),
]

down = [
    SQLStep("DROP TABLE testing"),
]
