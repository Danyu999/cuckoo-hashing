#include <stdlib.h>
#include <iostream>

#include "cuckoo-serial.h"

int main(int argc, char *argv[]) {
    int capacity = 15;
    int val = 222;
    CuckooSerialHashSet<int> *cuckoo_serial = new CuckooSerialHashSet<int>(capacity);
    cuckoo_serial->populate();
    cuckoo_serial->add(val);
    std::cout << "test successful: " << cuckoo_serial->contains(val) << std::endl;
}