import pandas as pd
import os

from DatabaseFunctions import DatabaseFunctions

class DatabaseUpdate(DatabaseFunctions):
    
    __slots__ = ("last_update_date")

    def __init__(self):
        self.last_update_date = None
        self.database_to_be_update = None

    def update_tw_price(self):
        pass