import myInfo as inf

def inMyRange(num, myRange):
    if inf.myHash <= myRange :
        return (num<=inf.myHash) and (num>=myRange)
    else:
        return (num<=inf.myHash) or (num>=myRange)