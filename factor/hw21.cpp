#include<string>
#include<iostream>
int main(){
  int num[5];
  for(int i = 0; i <= 4; i++){
    std::cin >> num[i];
  }
  for(int i = 0; i <= 4; i++){
    std::string st = "";
    for(int j = 0; j < num[i]; j++){
      st += "*";
    }
    std::cout << num[i] << "\t" << st << std::endl;
  
  }
}
