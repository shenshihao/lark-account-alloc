#!/usr/bin/env python3
"""解析低延时账号.txt，生成资金账号->二级营业部映射"""

import re
import json

mapping = {}  # {资金账号: 二级营业部}

with open('C:/账号梳理/低延时账号.txt', 'r', encoding='utf-8') as f:
    content = f.read()

# 按 低延时9691: 或 低延时96: 分割账号块
# 先统一替换
content = content.replace('低延时96:', '低延时9691:')
blocks = content.split('低延时9691:')

for block in blocks[1:]:  # 跳过第一个空块
    if not block.strip():
        continue

    # 提取资金账号
    fund_match = re.search(r'资金账号[：:]\s*(\S+)', block)
    # 提取二级营业部
    dept2_match = re.search(r'二级营业部[：:]\s*(\S+)', block)

    if fund_match and dept2_match:
        fund_id = fund_match.group(1).strip()
        dept2 = dept2_match.group(1).strip()
        mapping[fund_id] = dept2

print(f"解析到 {len(mapping)} 条映射关系")
for fund_id, dept2 in list(mapping.items())[:10]:
    print(f"  {fund_id} -> {dept2}")
print("...")

# 输出JSON供后续使用
with open('C:/账号梳理/低延时_dept2_mapping.json', 'w', encoding='utf-8') as f:
    json.dump(mapping, f, ensure_ascii=False, indent=2)
print(f"\n已保存到 C:/账号梳理/低延时_dept2_mapping.json")