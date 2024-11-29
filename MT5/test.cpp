#property copyright "Copyright 2024, MetaQuotes Ltd."
#property link      "https://www.mql5.com"
#property version   "1.00"
#include<Trade/Trade.mqh>

double init_size = 0.1;
double tp_point = 200;
ulong magic_number = 12321;
CTrade trade;
int OnInit()
  {
   double prev_size = init_size;
   trade.SetExpertMagicNumber(magic_number);
   return(INIT_SUCCEEDED);
  }

                  
void OnDeinit(const int reason)
  {

   
  }
void OnTick()
  {
   static bool traded = false;
   static string type_trade = 'B';
   static ENUM_SYMBOL_INFO_DOUBLE type_price = SYMBOL_BID if type_trade == 'B' else SYMBOL_ASK;
   static double price = 0;
   static double target_u = 0;
   static double targer_l = 0;
   double current = NormalizeDouble(SymbolInfoDouble(_Symbol, type_price), _Digits);
   if(!traded){
    trade.Buy(init_size, _Symbol, 0);
    price = NormalizeDouble(SymbolInfoDouble(_Symbol, type_price), _Digits);
    target_u = price + tp_point * _Point;
    target_l = price - tp_point * _Point;
    trade = true;
    }
    if(current >= target_u){
        if(type_trade == 'B'){
            target_u = current + tp_point * _Point;
            target_l = current - tp_point * _Point;
        }
        if(type_trade = 'S'){
            trade.Close();
        }
    }
   
  }
