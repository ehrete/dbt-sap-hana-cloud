class SapHanaCloudConnection:
    def __init__(self, handle, name):
        self.handle = handle  # The actual database connection handle
        self.name = name  # Connection name
        self.transaction_open = False  # Indicates if a transaction is open