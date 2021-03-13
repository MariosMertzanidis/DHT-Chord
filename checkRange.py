def inMyRange(num, myRange, myHash):
    if myHash >= myRange :
        return (int(num)<=myHash) and (int(num)>=myRange)
    else:
        return (int(num)<=myHash) or (int(num)>=myRange)