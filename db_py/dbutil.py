class dbutil(object):
    '''
    TWSE.py/get_TWSE_TWSE()
    TPEX.py/get_TWOTCI_TPEX()
    '''
    def convert_to_2024(da):
        year, month, day = da.split("/")
        return f"{int(year)}-{month}-{day}"