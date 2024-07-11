import warnings
from TWSE import TWSE
import os

warnings.filterwarnings("ignore")

# used: TWSE.py
class DatabaseCreation(TWSE):
    
    __slots__ = ("directories")
    os.chdir("../db")
    def __init__(self):
        super().__init__()
        
if __name__ == "__main__":
    obj = DatabaseCreation()
