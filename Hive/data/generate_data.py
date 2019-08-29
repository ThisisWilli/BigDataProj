import os
f = open("C:\\Users\\Wzf\\Desktop\\wc.txt", 'w', encoding='utf8')
count = 1
while count != 10000:
    f.write('hello,')
    f.write(str(count))
    f.write('\n')
    count += 1
f.close()