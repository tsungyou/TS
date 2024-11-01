## Strategies
1. MT5/JP225_Range_Breakout.mq5
2. MT5/JP225_RSI.mq5
3. MT5/JP225_RSI.mq5
4. MT5/Mean_reversion_USShares.mq5
5. Strategies/Turbulence.ipynb
6. Strategies/TWSE_f.ipynb

## Installation

```bash
python3 -m pip install requirements.txt
python3 -m pip install requirements-ML.txt
```

## Database Initialization/Update
```sh
cd database_mini/
sh db_init_mac.sh
sh daily_update.sh
```

## Scraping
```sh
cd database_mini/db_py
python3 Selenium.py
```
Selenium initilization for mac

## Factor Analysis
Factor Includes:
    1. ARIMA
    2. GARCH
    3. Fourier Series