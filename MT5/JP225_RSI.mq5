//+------------------------------------------------------------------+
//|                                               rsi_ma_jp_open.mq5 |
//|                                  Copyright 2024, MetaQuotes Ltd. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+

#include <Trade/Trade.mqh>
CTrade trade;
#property copyright "Copyright 2024, MetaQuotes Ltd."
#property link      "https://www.mql5.com"
#property version   "1.00"
//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+

int maHandle;
int rsiHandle;
int barsTotal;
int maDirection;
ENUM_APPLIED_PRICE price_type = PRICE_CLOSE;
ENUM_TIMEFRAMES timeframe = PERIOD_M5;
int ma_period = 5;
double vol_per = 1;
double sl_percentage = 0.005;
int OnInit()
  {
   
   trade.SetExpertMagicNumber(123321);
   maHandle = iMA(_Symbol, timeframe, ma_period, 0, MODE_SMA, price_type);
   rsiHandle = iRSI(_Symbol, timeframe, ma_period, price_type);
   barsTotal = iBars(_Symbol, timeframe);
//---
   return(INIT_SUCCEEDED);
  }
//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
//---
   
  }
//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
  {
   string da = TimeToString(TimeCurrent(), TIME_MINUTES);
   string da_h = StringSubstr(da, 0, 2);
   ulong h = StringToInteger(da_h);
   int bars = iBars(_Symbol, timeframe);
   if (barsTotal < bars) {
      barsTotal = bars;
      
      double prev_close = iClose("NAS100", PERIOD_D1, 1);
      double prev_open = iOpen("NAS100", PERIOD_D1, 1);
      double direction = (prev_close/prev_open);
      Comment(barsTotal, da);
      double cl = iClose(_Symbol, timeframe, 1);
      double ma[];
      CopyBuffer(maHandle, MAIN_LINE, 1, 1, ma);
         
      double rsi[];
      CopyBuffer(rsiHandle, MAIN_LINE, 1, 2, rsi);
      
      if (PositionsTotal() == 0 && h < 3){
         if(rsi[0] < 30 && rsi[1] > 30 && cl > ma[0] && direction > 1){
            trade.Buy(vol_per, _Symbol, 0, cl * (1 - sl_percentage), 0);
            //trade.Sell(vol_per, _Symbol, 0, cl * (1 + sl_percentage), 0);
         } else if (rsi[0] > 70 && rsi[1] < 70 && cl < ma[0] && direction < 1){
            trade.Sell(vol_per, _Symbol, 0, cl * (1 + sl_percentage), 0);
            //trade.Buy(vol_per, _Symbol, 0, cl * (1 - sl_percentage), 0);
         }
      
      } else if (PositionsTotal() == 0 && h > 3){
         Comment("nothing");
      } else if (PositionsTotal() != 0 && h > 8){
         for (int i = 0; i < PositionsTotal(); i++){
         ulong ticker = PositionGetTicket(i);
         PositionSelectByTicket(ticker);
         long type_trade = PositionGetInteger(POSITION_TYPE);
         trade.PositionClose(ticker);
        }
      }
      else if (PositionsTotal() == 0 && h < 3){
         for (int i = 0; i < PositionsTotal(); i++){
         ulong ticker = PositionGetTicket(i);
         PositionSelectByTicket(ticker);
         long type_trade = PositionGetInteger(POSITION_TYPE);
         if (type_trade == POSITION_TYPE_SELL){
            if (rsi[1] < 30){
               trade.PositionClose(ticker);
            }
         } else {
            if (rsi[1] > 70) {
               trade.PositionClose(ticker);
            }
         }
         }
         
         
      }
   }
   
   
  }
//+------------------------------------------------------------------+
