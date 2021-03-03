import myInfo as inf

def inMyRange(num, myRange):
    if inf.myHash <= myRange :
        return (int(num)<=inf.myHash) and (int(num)>=myRange)
    else:
        return (int(num)<=inf.myHash) or (int(num)>=myRange)