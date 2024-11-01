#include<iostream>
#include<string>
#include<vector>
#include<sstream>
#include<algorithm>
struct Triangle {
  std::string name;
  int a, b , c;
};

int main(){
  std::string str;
  std::vector<std::string> nontri;
  std::vector<std::string> acute;
  std::vector<std::string> obtuse;
  std::vector<std::string> right;

  std::vector<Triangle> triangles;

  while (true) {
    getline(std::cin, str);
    if(str == "-1"){break;}
    Triangle triangle;
    std::istringstream iss(str);
    iss >> triangle.name >> triangle.a >> triangle.b >> triangle.c;
    triangles.push_back(triangle);
  }
  for(int i = 0; i < triangles.size(); i++){
    Triangle triangle = triangles[i];
    if(triangle.a > triangle.b) std::swap(triangle.a, triangle.b);
    if(triangle.b > triangle.c) std::swap(triangle.b, triangle.c);
    if(triangle.a > triangle.b) std::swap(triangle.a, triangle.b);
    if(triangle.a + triangle.b <= triangle.c){nontri.push_back(triangle.name);}
    else{
      int aa = triangle.a * triangle.a;
      int bb = triangle.b * triangle.b;
      int cc = triangle.c * triangle.c;
      if(aa + bb < cc){obtuse.push_back(triangle.name);}
      else if(aa + bb > cc){acute.push_back(triangle.name);}
      else{right.push_back(triangle.name);}
    }
  }
  std::cout << "Not Triangle: " <<( nontri.empty() ? "None" : "");
  sort(nontri.begin(), nontri.end());
  for(size_t i = 0; i < nontri.size(); i++){
      if(i > 0) std::cout << ",";
      std::cout << nontri[i];
  }
  std::cout << std::endl;
  std::cout << "Acute Angle: " << (acute.empty() ? "None" : "");
  sort(acute.begin(), acute.end());
  for(size_t i = 0; i < acute.size(); i++){
    if(i > 0) std::cout << ",";
    std::cout << acute[i];
  }
  std::cout << std::endl;
  std::cout << "Obtuse Angle: " << (obtuse.empty() ? "None" : "");
  sort(obtuse.begin(), obtuse.end());
  for(size_t i = 0; i < obtuse.size(); i++){
    if(i > 0) std::cout << ",";
    std::cout << obtuse[i];
  }
  std::cout << std::endl;
  std::cout << "Right Angle: " << (right.empty() ? "None" : "");
  sort(right.begin(), right.end());
  for(size_t i = 0; i <right.size(); i++){
    if(i > 0) std::cout << ",";
    std::cout << right[i];
  }
  std::cout << std::endl;
}
