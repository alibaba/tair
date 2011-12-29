#include <list>
#include <iostream>

class TT {
public:
  int i ;
};

using namespace std;

int main(int argc, char *argv[]) {
  
  list<TT> ll;

  TT t;
  t.i = 1;
  ll.push_back(t);

  cout << ll.back().i << endl;
  t.i = 100;
  cout << ll.back().i << endl;
  return 0;
}

