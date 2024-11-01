# # Q1
# a = input()
# n, m = [eval(i) for i in a.split()]
# res = False
# for i in range(n):
#     chicken, rabbit = i, n - i
#     sum = chicken * 2 + rabbit * 4
#     if sum == m:
#         print("YES")
#         print(chicken, rabbit)
#         res = True
# if not res:
#     print("NO")


# # Q2
# a = input()
# l, s = [eval(i) for i in a.split()]
# iter = 0
# while True:
#     if s < l:
#         s += 5
#         iter += 1
#     elif l < s:
#         s -= 2
#         iter += 1
#     else:
#         break
# print(iter)

# # Q3
# a = int(input())

# if a % 7 == 0 or '7' in str(a):
#     print("YES")
# else:
#     print("NO")

# # Q4
# a = input()
# n, x, y = [eval(i) for i in a.split()]
# if n % 2 == 1:
#     n += 1
# iter = int(n/2)
# res = 20 + iter * x - (iter - 1) * y 
# print(res)


# # Q5
# a = int(input())
# rd_a = round(a, 2)
# ad_a = round(a ** (1/2) * 10, 2)
# diff = int(round(ad_a - rd_a, 0))
# print("Original:", "{:.2f}".format(rd_a))
# print("Adjusted: {:.2f}(+{})".format(ad_a, diff))


# # Q6
# a = input()
# if a == '1':
#     b = int(input())
#     if b <= 100 and b >= 60: print('pass')
#     elif b < 60: print('fail')
#     else: print('score error')
# elif a == '2':
#     b = int(input())
#     if b <= 100 and b >= 70: print('pass')
#     elif b < 70: print('fail')
#     else: print('score error')
# else:
#     print("role error")


# Q7
a = float(input())
b = float(input())
operator = input()
if operator == '/':
    res = a / b
elif operator == '+':
    res = a + b
elif operator == '*':
    res = a * b
elif operator == '-':
    res = a - b
print("{:.2f} {} {:.2f} = {:.2f}".format(a, operator, b, res))