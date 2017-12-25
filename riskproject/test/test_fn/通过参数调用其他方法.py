def AA(method, data):
    method(data)

def bb(data):
    print(11)

def cc(data):
    print(22)

AA(cc,'123')