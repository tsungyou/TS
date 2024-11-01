//+------------------------------------------------------------------+
//|                                               mean_reversion.mq5 |
//|                                  Copyright 2024, MetaQuotes Ltd. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#include <Math/Stat/Math.mqh>
#include <Trade/Trade.mqh>
CTrade trade;
#property copyright "Copyright 2024, MetaQuotes Ltd."
#property link      "https://www.mql5.com"
#property version   "1.00"
//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
  {
   trade.SetExpertMagicNumber(123123);
   //trade.Buy(0.1, _Symbol, 0, 0, 0);
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
void OnTick(){
   static int current_bar = 0;
   static double lot_size = 0.1;
   static int total = SymbolsTotal(true);
   if(current_bar != iBars(_Symbol, PERIOD_M1)){
      for (int i = total-1; i >= 0; i--){
         string symbolname = SymbolName(i, true);

         double cl1 = iClose("NAS100", PERIOD_M1, 1);
         double cl2 = iClose("NAS100", PERIOD_M1, 2);
         double cl3 = iClose("NAS100", PERIOD_M1, 3);
         double cl4 = iClose("NAS100", PERIOD_M1, 4);
         double cl5 = iClose("NAS100", PERIOD_M1, 5);
         double cl6 = iClose("NAS100", PERIOD_M1, 6);
         
         double d1 = (cl1 - cl2)/cl2;
         double d2 = (cl2 - cl3)/cl3;
         double d3 = (cl3 - cl4)/cl4;
         double d4 = (cl4 - cl5)/cl5;
         double d5 = (cl5 - cl6)/cl6;
         double cl1s = iClose(symbolname, PERIOD_M1, 1);
         double cl2s = iClose(symbolname, PERIOD_M1, 2);
         double cl3s = iClose(symbolname, PERIOD_M1, 3);
         double cl4s = iClose(symbolname, PERIOD_M1, 4);
         double cl5s = iClose(symbolname, PERIOD_M1, 5);
         double cl6s = iClose(symbolname, PERIOD_M1, 6);
         
         double d1s = (cl1s - cl2s)/cl2s;
         double d2s = (cl2s - cl3s)/cl3s;
         double d3s = (cl3s - cl4s)/cl4s;
         double d4s = (cl4s - cl5s)/cl5s;
         double d5s = (cl5s - cl6s)/cl6s;
         
         double s1 = d1 - d1s;
         double s2 = d2 - d2s;
         double s3 = d3 - d3s;
         double s4 = d4 - d4s;
         double s5 = d5 - d5s;
         
         double mean = (s1 + s2 + s3 + s4 + s5)/5;
         
         double array_[5];
         array_[0] = s1; 
         array_[1] = s2; 
         array_[2] = s3; 
         array_[3] = s4; 
         array_[4] = s5; 
         
         double std = MathStandardDeviation(array_);
         double signal = (s1 - mean * 100) / (std * 100);
         //Print(symbolname, DoubleToString(signal));
         if(MathAbs(signal) >= 2){
            if(d1s > 0 && d2s > 0  && d3s > 0  && d4s > 0  && d5s > 0){
               double ask = SymbolInfoDouble(symbolname, SYMBOL_BID);
               trade.Sell(lot_size, symbolname, 0, ask * 1.005, ask * 0.995);
            } else if(d1s < 0 && d2s < 0  && d3s < 0  && d4s < 0  && d5s < 0){
               double ask = SymbolInfoDouble(symbolname, SYMBOL_ASK);
               trade.Buy(lot_size, symbolname, 0, ask * 0.995, ask * 1.005);
            } else {
               Print(symbolname + " has std over 2 but cont candles condition doesn't satisfied");
            }
            double margin1;
            double margin2;
            SymbolInfoMarginRate(symbolname, ORDER_TYPE_BUY, margin1, margin2);
            Print(symbolname + " " + DoubleToString(margin1));
         }
         current_bar = iBars(_Symbol, PERIOD_M1);
      } 
      Sleep(30);
    } else {
    Comment("Not yet " + TimeToString(TimeCurrent()));
    }
}