#include<string>
#include<iostream>
int main(){
  int n;
  std::cin >> n;
  int arr[n];
  for(int i = 0; i <n; i++){
    std::cin >> arr[i];
  }
  int output;
  if(n % 2 == 0){
    int a = n / 2;
    int b = a - 1;
    
    int f = arr[a] + arr[b];
    if(f % 2 == 1){f = ((int)f + 1)/2;}
    else{f = f / 2;}
    std::cout << f << std::endl;   
  }else{
    int a = n / 2;
    std::cout << arr[a] << std::endl;
  }
}
