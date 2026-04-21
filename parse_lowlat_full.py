#!/usr/bin/env python3
"""解析低延时账号.txt，生成完整的账号数据映射"""

import re
import json

mapping = {}  # {资金账号: {密文, agw用户名/密码}}

with open('C:/账号梳理/低延时账号.txt', 'r', encoding='utf-8') as f:
    content = f.read()

# 按 低延时9691: 或 低延时96: 分割账号块
content = content.replace('低延时96:', '低延时9691:')
blocks = content.split('低延时9691:')

for block in blocks[1:]:  # 跳过第一个空块
    if not block.strip():
        continue

    # 提取资金账号
    fund_match = re.search(r'资金账号[：:]\s*(\S+)', block)
    # 提取密文
    cipher_match = re.search(r'密文[：:]\s*(\S+)', block)
    # 提取agw用户名/密码
    agw_match = re.search(r'agw用户名/密码[：:]\s*(\S+)', block)

    if fund_match:
        fund_id = fund_match.group(1).strip()
        mapping[fund_id] = {
            '密文': cipher_match.group(1).strip() if cipher_match else '',
            'agw用户名/密码': agw_match.group(1).strip() if agw_match else ''
        }

print(f"解析到 {len(mapping)} 条映射关系")
for fund_id, data in list(mapping.items())[:5]:
    print(f"  {fund_id}: 密文={data['密文'][:20] if data['密文'] else 'N/A'}..., agw={data['agw用户名/密码']}")
print("...")

# 输出JSON供后续使用
with open('C:/账号梳理/低延时_full_mapping.json', 'w', encoding='utf-8') as f:
    json.dump(mapping, f, ensure_ascii=False, indent=2)
print(f"\n已保存到 C:/账号梳理/低延时_full_mapping.json")