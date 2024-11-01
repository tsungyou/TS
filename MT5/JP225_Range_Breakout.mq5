#include<Trade/Trade.mqh>
CTrade trade;
double vol = 0.1;
double sl_tp = 0.005;
int OnInit()
  {
   MqlDateTime tm = {};
   datetime dt = TimeCurrent();
   int dow = tm.day_of_week;
   if(dow == 0){Comment("123123123");}
   
   for(int i = 1; i <= 50; i++){
      string objName = "obj" + IntegerToString(i);
      datetime t1 = iTime(_Symbol, PERIOD_W1, i);
   
      ObjectCreate(0, objName, OBJ_VLINE, 0, t1, 0);
      ObjectSetInteger(0, objName, OBJPROP_COLOR, clrAqua);
   }
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
   MqlDateTime tm = {};
   datetime dt = TimeCurrent();
   int dow = tm.day_of_week;
   Comment(dow);
   if(dow == 0){
      double prevHigh = iHigh(_Symbol, PERIOD_W1, 1);
      double prevLow = iLow(_Symbol, PERIOD_W1, 1);
      double prevOpen = iOpen(_Symbol, PERIOD_W1, 1);
      double prevClose = iClose(_Symbol, PERIOD_W1, 1);
      double tick = SymbolInfoDouble(_Symbol, SYMBOL_POINT);
      Comment(tick);
      trade.BuyStop(vol, prevHigh, _Symbol, prevHigh*(1 - sl_tp), prevHigh*(1+sl_tp), ORDER_TIME_DAY);
      trade.SellStop(vol, prevLow, _Symbol, prevLow*(1 + sl_tp), prevLow*(1-sl_tp), ORDER_TIME_DAY);
   }
   
  }
//+------------------------------------------------------------------+
