import sys
import os


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

print('正在处理区域选择---')
os.system(r'cd /Users/zoetsou/PycharmProjects/work/shell_rent_house && python3 1_getAreaUrl.py')
print('正在处理区域下的房子数量--并判断是否继续向下区分')
os.system(r'cd /Users/zoetsou/PycharmProjects/work/shell_rent_house && python3 2_judgeNum.py')
print('现在开始整理页数url,获取所有的页url--')
os.system(r'cd /Users/zoetsou/PycharmProjects/work/shell_rent_house && python3 3_getPage.py')
print('现在开始处理列表页数据--')
os.system(r'cd /Users/zoetsou/PycharmProjects/work/shell_rent_house && python3 4_getList.py')
print('现在开始获取详情页数据---')
os.system(r'cd /Users/zoetsou/PycharmProjects/work/shell_rent_house && python3 5_getDetail.py')
print('现在开始导出数据到excel----')
os.system(r'cd /Users/zoetsou/PycharmProjects/work/shell_rent_house && python3 6_toExcel.py')
