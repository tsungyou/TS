from DbCreation import DatabaseCreation
from DbFunctions import DatabaseFunctions
import pandas as pd
import numpy as np
import os

class DatabaseUpdate(DatabaseCreation, DatabaseFunctions):
    __slots__ = ("test_slots")

    def __init__(self):
        super().__init__()
    

    def test(self):
        print("test")




if __name__ == "__main__":
    obj = DatabaseUpdate()
    print(obj.__slots__)