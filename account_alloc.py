#!/usr/bin/env python3
"""
账号分配脚本 - 数据查询
从飞书多维表格获取账号数据，用于后续模板生成
"""

import sys
import json
import time

BASE_TOKEN = "AdSxbgrTgaVZY7sGGJicufPxn2F"

# 缓存配置
# Base数据缓存（分配后失效）
_base_cache = {}

# systemid映射表缓存
_systemid_map = None
_systemid_loaded = False

def load_systemid_map(force=False):
    """加载syetemid.txt文件，构建fundid->systemid映射"""
    global _systemid_map, _systemid_loaded
    if _systemid_loaded and not force:
        return _systemid_map

    _systemid_map = {}
    try:
        with open('/home/admin/config/syetemid.txt', 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 2:
                    systemid = parts[0]
                    fundid = parts[1]
                    _systemid_map[fundid] = systemid
    except Exception as e:
        print("Warning: failed to load systemid file: {}".format(e))
    _systemid_loaded = True
    return _systemid_map

def reload_systemid():
    """重新加载systemid文件"""
    global _systemid_loaded
    _systemid_loaded = False
    return load_systemid_map()

def invalidate_cache():
    """失效Base缓存，下次查询时重新加载"""
    global _base_cache
    _base_cache = {}

def lookup_systemid(营业部, 资金账号):
    """根据营业部+资金账号查找systemid"""
    load_systemid_map()

    # 资金账号格式: 03 + orgid(4位) + fundid(10位) = 16位
    # systemid.txt格式: orgid(4位) + "_" + fundid(后8位)
    fundid_without_03 = str(资金账号).lstrip('0')
    # 取后8位作为short fundid
    fundid_short = fundid_without_03[-8:] if len(fundid_without_03) >= 8 else fundid_without_03.zfill(8)
    key = "{}_{}".format(营业部, fundid_short)

    if key in _systemid_map:
        return _systemid_map[key]
    return "待补充"

def get_table_records(table_name_or_id):
    """通过lark-cli查询表记录（带缓存，分配后失效）"""
    global _base_cache

    # 检查缓存
    if table_name_or_id in _base_cache:
        return _base_cache[table_name_or_id]["records"], \
               _base_cache[table_name_or_id]["fields"], \
               _base_cache[table_name_or_id]["record_ids"]

    # 重新获取
    import subprocess
    cmd = "lark-cli base +record-list --base-token {} --table-id \"{}\" --limit 500".format(BASE_TOKEN, table_name_or_id)
    result = subprocess.run(cmd, capture_output=True, shell=True)
    if result.returncode != 0:
        return [], [], []
    try:
        data = json.loads(result.stdout.decode('utf-8', errors='replace'))
        if data.get("ok"):
            records = data.get("data", {}).get("data", [])
            fields = data.get("data", {}).get("fields", [])
            record_ids = data.get("data", {}).get("record_id_list", [])
            # 更新缓存
            _base_cache[table_name_or_id] = {
                "records": records,
                "fields": fields,
                "record_ids": record_ids
            }
            return records, fields, record_ids
    except:
        pass
    return [], [], []

def is_allocated(record, field_names):
    """检查记录是否已分配"""
    for i, fn in enumerate(field_names):
        if fn == "是否已分配" and i < len(record):
            val = record[i]
            if val and ("已分配" in str(val) or "已分配" in str(val[0]) if isinstance(val, list) else False):
                return True
    return False

def search_records(table_name_or_id, query, search_field, need_systemid=True):
    """在指定表中搜索，返回(记录字典, record_id)"""
    records, field_names, record_ids = get_table_records(table_name_or_id)

    for idx, record in enumerate(records):
        for i, field_name in enumerate(field_names):
            if field_name == search_field and i < len(record):
                if query in str(record[i]):
                    # 跳过已分配的记录
                    if is_allocated(record, field_names):
                        continue
                    record_dict = {}
                    for j, fn in enumerate(field_names):
                        if j < len(record):
                            record_dict[fn] = record[j]
                    record_id = record_ids[idx] if idx < len(record_ids) else None
                    enrich_with_systemid(record_dict, need_systemid)
                    return record_dict, record_id
    return None, None

def enrich_with_systemid(record_dict, need_systemid=True):
    """根据营业部+资金账号查找systemid并添加到记录"""
    if not need_systemid:
        record_dict["systemid"] = None
        return record_dict

    营业部 = record_dict.get("营业部", "")
    资金账号 = record_dict.get("资金账号", "")
    if 营业部 and 资金账号:
        record_dict["systemid"] = lookup_systemid(str(营业部), str(资金账号))
    else:
        record_dict["systemid"] = "待补充"
    return record_dict

def search_by_cust_id(query, table_name_or_id, need_systemid=True):
    """在指定表中搜索客户号"""
    return search_records(table_name_or_id, query, "客户号", need_systemid)

def search_lowlat_by_fund_id(query, need_systemid=True):
    """搜索低延时表通过资金账号"""
    record, record_id = search_records("低延时账号表", query, "资金账号", need_systemid)
    return record, record_id

def search_lowlat_by_cust_id(query, need_systemid=True):
    """搜索低延时表通过客户号"""
    return search_by_cust_id(query, "低延时账号表", need_systemid)

def search_dingdian(query, need_systemid=False):
    """搜索顶点表通过客户号，返回(记录字典, record_id, 类型)"""
    # 先搜两融
    record, record_id = search_by_cust_id(query, "顶点两融账号表", need_systemid)
    if record:
        record["类型"] = "两融"
        return record, record_id, "顶点两融账号表"
    # 再搜现货
    record, record_id = search_by_cust_id(query, "顶点现货账号表", need_systemid)
    if record:
        record["类型"] = "现货"
        return record, record_id, "顶点现货账号表"
    return None, None, None

def get_first_unallocated(table_name_or_id, need_systemid=True):
    """获取表中第一条未分配的记录"""
    records, field_names, record_ids = get_table_records(table_name_or_id)
    for idx, record in enumerate(records):
        if not is_allocated(record, field_names):
            record_dict = {}
            for j, fn in enumerate(field_names):
                if j < len(record):
                    record_dict[fn] = record[j]
            record_id = record_ids[idx] if idx < len(record_ids) else None
            enrich_with_systemid(record_dict, need_systemid)
            return record_dict, record_id
    return None, None

def get_first_unallocated_dingdian(need_systemid=False):
    """获取第一条未分配的顶点账号"""
    # 先尝试两融
    record, record_id = get_first_unallocated("顶点两融账号表", need_systemid)
    if record:
        record["类型"] = "两融"
        return record, record_id, "顶点两融账号表"
    # 再尝试现货
    record, record_id = get_first_unallocated("顶点现货账号表", need_systemid)
    if record:
        record["类型"] = "现货"
        return record, record_id, "顶点现货账号表"
    return None, None, None

def get_first_unallocated_lowlat(need_systemid=True):
    """获取第一条未分配的低延时账号"""
    return get_first_unallocated("低延时账号表", need_systemid)

def mark_allocated(table_name_or_id, record_id):
    """标记记录为已分配，并失效缓存"""
    if not record_id:
        return False
    import subprocess
    cmd = "lark-cli base +record-upsert --base-token {} --table-id \"{}\" --record-id {} --json \"{{\\\"是否已分配\\\":\\\"已分配\\\"}}\"".format(
        BASE_TOKEN, table_name_or_id, record_id)
    result = subprocess.run(cmd, capture_output=True, shell=True)
    if result.returncode == 0:
        invalidate_cache()  # 分配后失效缓存
    return result.returncode == 0

def parse_access_type(access_type_str):
    """解析接入类型字符串，返回是否需要查询systemid"""
    if access_type_str in ["直连"]:
        return False
    elif access_type_str in ["统一接入", "三方接入"]:
        return True
    else:
        # 默认统一接入
        return True

def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python account_alloc.py auto <类型> [接入类型]  # 自动分配")
        print("    类型: dingdian / lowlat")
        print("    接入类型: 直连 / 统一接入 / 三方接入")
        print("    示例: python account_alloc.py auto dingdian 直连")
        print("    示例: python account_alloc.py auto lowlat 统一接入")
        print("")
        print("  python account_alloc.py search <类型> <客户号> [接入类型]")
        print("    示例: python account_alloc.py search dingdian 2105416 直连")
        print("")
        print("  python account_alloc.py alloc <类型> <客户号> [接入类型]")
        print("    分配并标记为已分配")
        print("")
        print("  python account_alloc.py reload  # 重新加载systemid文件")
        print("  python account_alloc.py list    # 列出各表未分配记录数")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "reload":
        reload_systemid()
        print("systemid文件已重新加载")
        return

    if mode == "list":
        tables = ["顶点两融账号表", "顶点现货账号表", "低延时账号表"]
        for table in tables:
            records, fields, _ = get_table_records(table)
            unallocated = sum(1 for r in records if not is_allocated(r, fields))
            print("{}: {} 条记录 (未分配: {})".format(table, len(records), unallocated))
        return

    if mode == "auto":
        if len(sys.argv) < 3:
            print("用法: python account_alloc.py auto <类型> [接入类型]")
            print("类型: dingdian / lowlat")
            print("接入类型: 直连 / 统一接入 / 三方接入（默认统一接入）")
            sys.exit(1)
        acc_type = sys.argv[2]
        access_type = sys.argv[3] if len(sys.argv) > 3 else "统一接入"
        need_systemid = parse_access_type(access_type)

        if acc_type == "dingdian":
            record, record_id, table_name = get_first_unallocated_dingdian(need_systemid)
            if record:
                print("找到顶点账号 - {} ({}):".format(record.get('类型'), access_type))
                print(json.dumps(record, ensure_ascii=False, indent=2))
                print("record_id: {}".format(record_id))
            else:
                print("没有未分配的顶点账号")
        elif acc_type == "lowlat":
            record, record_id = get_first_unallocated_lowlat(need_systemid)
            if record:
                print("找到低延时账号 ({}):".format(access_type))
                print(json.dumps(record, ensure_ascii=False, indent=2))
                print("record_id: {}".format(record_id))
            else:
                print("没有未分配的低延时账号")
        else:
            print("未知类型: {}".format(acc_type))
        return

    if mode == "search":
        if len(sys.argv) < 4:
            print("用法: python account_alloc.py search <类型> <客户号> [接入类型]")
            sys.exit(1)
        acc_type = sys.argv[2]
        query = sys.argv[3]
        access_type = sys.argv[4] if len(sys.argv) > 4 else "统一接入"
        need_systemid = parse_access_type(access_type)

        if acc_type == "dingdian":
            record, record_id, table_name = search_dingdian(query, need_systemid)
            if record:
                print("找到顶点账号 - {} ({}):".format(record.get('类型'), access_type))
                print(json.dumps(record, ensure_ascii=False, indent=2))
                print("record_id: {}".format(record_id))
            else:
                print("未找到未分配的顶点账号: {}".format(query))
        elif acc_type == "lowlat":
            record, record_id = search_lowlat_by_cust_id(query, need_systemid)
            if not record:
                record, record_id = search_lowlat_by_fund_id(query, need_systemid)
            if record:
                print("找到低延时账号 ({}):".format(access_type))
                print(json.dumps(record, ensure_ascii=False, indent=2))
                print("record_id: {}".format(record_id))
            else:
                print("未找到未分配的低延时账号: {}".format(query))
        else:
            print("未知类型: {}".format(acc_type))
        return

    if mode == "alloc":
        if len(sys.argv) < 4:
            print("用法: python account_alloc.py alloc <类型> <客户号> [接入类型]")
            sys.exit(1)
        acc_type = sys.argv[2]
        query = sys.argv[3]
        access_type = sys.argv[4] if len(sys.argv) > 4 else "统一接入"
        need_systemid = parse_access_type(access_type)

        if acc_type == "dingdian":
            record, record_id, table_name = search_dingdian(query, need_systemid)
            if record:
                success = mark_allocated(table_name, record_id)
                if success:
                    print("已分配并标记: 顶点{} {} (客户号: {})".format(record.get('类型'), access_type, query))
                else:
                    print("分配成功但标记失败")
            else:
                print("未找到未分配的顶点账号: {}".format(query))
        elif acc_type == "lowlat":
            record, record_id = search_lowlat_by_cust_id(query, need_systemid)
            if not record:
                record, record_id = search_lowlat_by_fund_id(query, need_systemid)
            if record:
                success = mark_allocated("低延时账号表", record_id)
                if success:
                    print("已分配并标记: 低延时96 {} (客户号/资金账号: {})".format(access_type, query))
                else:
                    print("分配成功但标记失败")
            else:
                print("未找到未分配的低延时账号: {}".format(query))
        else:
            print("未知类型: {}".format(acc_type))
        return

    # 兼容旧命令格式
    if mode == "search_old":
        # 旧: python account_alloc.py search <账号>
        query = sys.argv[2]
        record, record_id, table_name = search_dingdian(query, False)
        if record:
            print("找到顶点账号 - {}:".format(record.get('类型')))
            print(json.dumps(record, ensure_ascii=False, indent=2))
            print("record_id: {}".format(record_id))
            return
        record, record_id = search_lowlat_by_cust_id(query, True)
        if not record:
            record, record_id = search_lowlat_by_fund_id(query, True)
        if record:
            print("找到低延时账号:")
            print(json.dumps(record, ensure_ascii=False, indent=2))
            print("record_id: {}".format(record_id))
            return
        print("未找到未分配的账号: {}".format(query))

if __name__ == "__main__":
    main()