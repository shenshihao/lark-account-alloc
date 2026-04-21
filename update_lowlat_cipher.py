#!/usr/bin/env python3
"""更新飞书多维表格中低延时账号表的密文和agw用户名/密码"""

import json
import subprocess

BASE_TOKEN = "AdSxbgrTgaVZY7sGGJicufPxn2F"
TABLE_ID = "低延时账号表"

# 加载完整映射
with open('C:/账号梳理/低延时_full_mapping.json', 'r', encoding='utf-8') as f:
    mapping = json.load(f)

print(f"加载了 {len(mapping)} 条映射")

# 分页获取所有记录（每次最多返回200条）
all_records = []
all_fields = None
all_record_ids = []
offset = 0
limit = 500

while True:
    cmd = 'lark-cli base +record-list --base-token {} --table-id "{}" --limit {} --offset {} --field-id 资金账号 --field-id 密文 --field-id agw用户名/密码 --field-id ID'.format(
        BASE_TOKEN, TABLE_ID, limit, offset
    )
    result = subprocess.run(cmd, capture_output=True, shell=True, cwd='C:/Users/Administrator/.claude/skills/lark-account-alloc')
    if result.returncode != 0:
        print("获取记录失败:", result.stderr.decode('utf-8', errors='replace'))
        break

    try:
        data = json.loads(result.stdout.decode('utf-8', errors='replace'))
    except:
        print("解析失败")
        break

    if not data.get("ok"):
        print("获取记录失败")
        break

    records = data.get("data", {}).get("data", [])
    fields = data.get("data", {}).get("fields", [])
    record_ids = data.get("data", {}).get("record_id_list", [])

    if all_fields is None:
        all_fields = fields

    all_records.extend(records)
    all_record_ids.extend(record_ids)

    has_more = data.get("data", {}).get("has_more", False)
    if not has_more or len(records) == 0:
        break

    offset += 200  # API每次最多返回200条
    print(f"  已获取 {len(all_records)} 条记录...")

print(f"获取到 {len(all_records)} 条记录")

# 找到字段索引
fund_idx = all_fields.index("资金账号") if "资金账号" in all_fields else -1
cipher_idx = all_fields.index("密文") if "密文" in all_fields else -1
agw_idx = all_fields.index("agw用户名/密码") if "agw用户名/密码" in all_fields else -1

print(f"资金账号索引: {fund_idx}, 密文索引: {cipher_idx}, agw用户名/密码索引: {agw_idx}")

# 统计更新
updated = 0
skipped = 0
errors = 0
not_in_mapping = 0

for idx, record in enumerate(all_records):
    if idx >= len(all_record_ids):
        continue

    fund_id = str(record[fund_idx]) if fund_idx >= 0 and fund_idx < len(record) and record[fund_idx] else ""
    current_cipher = str(record[cipher_idx]) if cipher_idx >= 0 and cipher_idx < len(record) and record[cipher_idx] else ""
    current_agw = str(record[agw_idx]) if agw_idx >= 0 and agw_idx < len(record) and record[agw_idx] else ""
    record_id = all_record_ids[idx]

    if not fund_id:
        continue

    if fund_id in mapping:
        new_cipher = mapping[fund_id]['密文']
        new_agw = mapping[fund_id]['agw用户名/密码']

        # 如果密文或agw有变化就更新
        if current_cipher != new_cipher or current_agw != new_agw:
            update_cmd = 'lark-cli base +record-upsert --base-token {} --table-id "{}" --record-id {} --json "{{\\"密文\\":\\"{}\\",\\"agw用户名/密码\\":\\"{}\\"}}"'.format(
                BASE_TOKEN, TABLE_ID, record_id, new_cipher, new_agw
            )
            result = subprocess.run(update_cmd, capture_output=True, shell=True, cwd='C:/Users/Administrator/.claude/skills/lark-account-alloc')
            if result.returncode == 0:
                print(f"更新 record_id={record_id}, 资金账号={fund_id}")
                updated += 1
            else:
                print(f"更新失败 record_id={record_id}: {result.stderr.decode('utf-8', errors='replace')[:100]}")
                errors += 1
        else:
            skipped += 1
    else:
        not_in_mapping += 1

print(f"\n更新完成: 成功 {updated}, 已是最新 {skipped}, 无映射 {not_in_mapping}, 错误 {errors}")