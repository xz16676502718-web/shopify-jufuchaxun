import os
import re
import json
import time
import threading
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================== 配置区 ====================
STORES_JSON_PATH = "stores.json"
TOKEN_CACHE_FILE = "token_cache.json"

# WebApp 部署地址
GAS_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbz5lkaskmNJRA_X0iL_QQyRabeLzmnWmtuTETu3TrUEew7UOXzSd-ccas2yK0h68GE/exec"
API_VERSION = "2026-01"
MAX_WORKERS = 3  # 保持 3 并发，避免高并发断连

FILE_LOCK = threading.Lock()

# 匹配所有包含日期开头的拒付标签（例如 "9.10-欺诈"、"9.16截止-欺诈"、"8.26-未收到产品-已发邮件" 等）
DISPUTE_TAG_PATTERN = re.compile(r'^(\d{1,2}\.\d{1,2})(?:截止)?-(.+)$')

# 仅用于 Shopify 订单打了简短标签的映射
SHORT_REASON_MAP = {
    "fraudulent": "欺诈",
    "product_not_received": "未收到产品",
    "product_unacceptable": "产品不可接受",
    "unrecognized": "账户详细信息不正确",
    "credit_not_processed": "退款未处理",
    "duplicate": "重复付款",
    "general": "通用",
    "subscription_canceled": "订阅已取消",
    "canceled": "其他"
}

# 保留用于表格导出（N列）的原始详细拒付原因映射
REASON_MAP = {
    "fraudulent": "欺诈/未授权交易",
    "product_not_received": "未收到货物",
    "product_unacceptable": "货不对板/商品缺陷",
    "unrecognized": "账单无法识别",
    "credit_not_processed": "退款未处理",
    "duplicate": "重复扣款",
    "general": "常规原因/其他",
    "subscription_canceled": "订阅已被取消",
    "canceled": "订单已被取消"
}

TYPE_MAP = {"CHARGEBACK": "拒付", "INQUIRY": "调单/问询"}
STATUS_MAP = {
    "NEEDS_RESPONSE": "待回复", "UNDER_REVIEW": "审查中",
    "WON": "申诉成功(胜诉)", "LOST": "申诉失败(败诉)",
    "ACCEPTED": "已接受(放弃申诉)", "CHARGE_REFUNDED": "已全额退款"
}

def get_session():
    return requests.Session()

# ==================== 安全请求包装函数 ====================
def safe_request(session, method, url, max_retries=5, **kwargs):
    for attempt in range(1, max_retries + 1):
        try:
            kwargs.setdefault("timeout", 20)
            resp = session.request(method, url, **kwargs)
            
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "2")
                try:
                    wait_time = float(retry_after)
                except ValueError:
                    wait_time = 2.0
                time.sleep(wait_time + 0.5)
                continue
            elif resp.status_code in [500, 502, 503, 504]:
                time.sleep(2 * attempt)
                continue
                
            return resp
            
        except (requests.exceptions.RequestException, Exception) as e:
            if attempt == max_retries:
                raise e
            time.sleep(2 * attempt)
            
    return None

# ==================== 错误日志自动翻译引擎 ====================
def translate_error(error_type, http_status, error_details):
    details_str = str(error_details).lower()
    
    if http_status == 429 or "exceeded" in details_str or "rate limit" in details_str:
        return "【API限流 (429)】请求频率超出Shopify限制，已自动退避重试。"
    elif http_status == 401 or "token" in details_str or "autherror" in str(error_type).lower():
        return "【密钥认证失败 (401)】Client ID / Secret 填写错误或 Token 已失效。"
    elif http_status == 403 or "forbidden" in details_str:
        return "【无接口权限 (403)】当前 App 密钥缺乏 Shopify Payments 或 Orders 的读取权限。"
    elif "ssleoferror" in details_str or "ssl" in details_str or "eof" in details_str:
        return "【SSL连接中断】网络抖动或并发过高被 Shopify 防火墙切断，脚本已自动重试。"
    elif "field" in details_str and "doesn't exist" in details_str:
        return "【API语法错误】请求中包含了当前 API 版本已废弃或不存在的字段。"
    elif http_status >= 500:
        return f"【Shopify服务端故障 ({http_status})】Shopify 官方服务器暂时崩溃，稍后重试即可。"
    else:
        return f"【网络/未知异常】{error_type}: {str(error_details)[:120]}"

def parse_dispute_date(evidence_due_date):
    """从截止日期中解析出月.日格式"""
    if evidence_due_date and evidence_due_date != "无":
        try:
            dt = datetime.fromisoformat(evidence_due_date.replace("Z", "+00:00"))
            return f"{dt.month}.{dt.day}"
        except Exception:
            parts = str(evidence_due_date).split("T")[0].split("-")
            if len(parts) == 3:
                return f"{int(parts[1])}.{int(parts[2])}"
    return ""

def process_order_tags(tags_list, target_date_str, target_reason_cn):
    """
    智能更新标签：
    1. 扫描原有标签，提取自定义后缀（如'-已发邮件'）；
    2. 统一词汇并原地修改，删除重复的旧拒付标签；
    3. 完好保留非拒付备注标签。
    """
    other_tags = []
    collected_suffixes = []

    for tag in tags_list:
        match = DISPUTE_TAG_PATTERN.match(tag)
        if match:
            rest = match.group(2)
            parts = rest.split("-")
            if len(parts) > 1:
                for suffix in parts[1:]:
                    s = suffix.strip()
                    if s and s not in collected_suffixes:
                        collected_suffixes.append(s)
        else:
            other_tags.append(tag)

    if target_reason_cn:
        base_tag = f"{target_date_str}-{target_reason_cn}" if target_date_str else target_reason_cn
        if collected_suffixes:
            final_dispute_tag = f"{base_tag}-" + "-".join(collected_suffixes)
        else:
            final_dispute_tag = base_tag
        return other_tags + [final_dispute_tag]
    
    return other_tags

# ==================== Token 缓存读写 ====================
def load_token_cache():
    if os.path.exists(TOKEN_CACHE_FILE):
        try:
            with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_token_cache(cache):
    with FILE_LOCK:
        try:
            dir_name = os.path.dirname(TOKEN_CACHE_FILE)
            if dir_name:  # 只有当包含子目录时才创建文件夹
                os.makedirs(dir_name, exist_ok=True)
            with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"写入 Token 缓存文件失败: {e}")

def get_access_token(session, store, cache):
    domain = store["domain"]
    client_id = store["client_id"]
    client_secret = store["client_secret"]

    if domain in cache and "access_token" in cache[domain]:
        return cache[domain]["access_token"]

    token_url = f"https://{domain}/admin/oauth/access_token"
    payload = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}
    
    try:
        resp = safe_request(session, "POST", token_url, json=payload, timeout=15)
        if resp and resp.status_code == 200:
            token_data = resp.json()
            access_token = token_data.get("access_token")
            if access_token:
                cache[domain] = {"access_token": access_token}
                save_token_cache(cache)
                return access_token
    except Exception as e:
        print(f"[{domain}] 请求 Token 异常: {e}")
        
    return None

# ==================== 获取订单详情（含智能改标 + 物流提取） ====================
def get_order_details(session, shop_domain, access_token, order_id, evidence_due_date="", raw_reason=""):
    if not order_id:
        return "N/A", 0.0, "N/A", "N/A", [], "", "无", "无"
    
    url = f"https://{shop_domain}/admin/api/{API_VERSION}/orders/{order_id}.json?fields=id,name,total_price,customer,refunds,tags,fulfillments"
    headers = {"X-Shopify-Access-Token": access_token}
    try:
        res = safe_request(session, "GET", url, headers=headers, timeout=15)
        if res and res.status_code == 200:
            ord_data = res.json().get("order", {})
            order_name = ord_data.get("name", "N/A")
            order_total = float(ord_data.get("total_price", 0.0))
            cust = ord_data.get("customer") or {}
            cust_name = f"{cust.get('first_name', '')} {cust.get('last_name', '')}".strip() or "N/A"
            cust_email = cust.get('email', '') or "N/A"
            refunds = ord_data.get("refunds", [])
            
            raw_tags = ord_data.get("tags", "")
            tags_list = [t.strip() for t in raw_tags.split(",") if t.strip()] if raw_tags else []
            
            tag_date_str = parse_dispute_date(evidence_due_date)
            target_reason_cn = SHORT_REASON_MAP.get(str(raw_reason).lower(), "其他")
            
            # 智能更正与归并标签（仅影响订单标签）
            new_tags_list = process_order_tags(tags_list, tag_date_str, target_reason_cn)
            
            # 使用 set 对比，避免因为列表标签顺序不同而触发不必要的 API 更新
            if set(new_tags_list) != set(tags_list):
                new_tags_str = ", ".join(new_tags_list)
                update_url = f"https://{shop_domain}/admin/api/{API_VERSION}/orders/{order_id}.json"
                update_headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}
                update_payload = {"order": {"id": order_id, "tags": new_tags_str}}
                
                update_res = safe_request(session, "PUT", update_url, headers=update_headers, json=update_payload, timeout=15)
                if update_res and update_res.status_code == 200:
                    tags_list = new_tags_list
                    print(f"[{shop_domain}] 订单 {order_name} 标签已更正更新为: {new_tags_str}")

            formatted_tags = "\n".join(tags_list)
            
            # 解析物流承运商与单号
            fulfillments = ord_data.get("fulfillments", [])
            carriers = []
            tracking_numbers = []
            
            for f in fulfillments:
                company = f.get("tracking_company")
                if company and str(company).strip() not in carriers:
                    carriers.append(str(company).strip())
                
                t_num = f.get("tracking_number")
                if t_num and str(t_num).strip() not in tracking_numbers:
                    tracking_numbers.append(str(t_num).strip())
                for num in f.get("tracking_numbers") or []:
                    n_str = str(num).strip()
                    if n_str and n_str not in tracking_numbers:
                        tracking_numbers.append(n_str)
                        
            carrier_str = "\n".join(carriers) if carriers else "无"
            tracking_str = "\n".join(tracking_numbers) if tracking_numbers else "无"
            
            return order_name, order_total, cust_name, cust_email, refunds, formatted_tags, carrier_str, tracking_str
    except Exception as e:
        print(f"[{shop_domain}] 获取订单 {order_id} 详情异常: {e}")
        
    return "N/A", 0.0, "N/A", "N/A", [], "", "无", "无"

# ==================== 单个店铺 API 抓取逻辑 ====================
def fetch_disputes_for_shop(store, token_cache):
    shop_name = store.get("name", store["domain"])
    shop_domain = store["domain"]
    session = get_session()
    
    access_token = get_access_token(session, store, token_cache)
    parsed_disputes = []
    error_logs = []
    
    if not access_token:
        err_msg = "无法通过 Client ID/Secret 换取 Access Token"
        explanation = translate_error("AuthError", 401, err_msg)
        error_logs.append({
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "shop_name": shop_name,
            "shop_domain": shop_domain,
            "api": "OAuth Token Exchange",
            "http_status": 401,
            "error_type": "AuthError",
            "error_details": err_msg,
            "retry_count": 5,
            "error_explanation": explanation
        })
        sync_status = {
            "shop_name": shop_name,
            "shop_domain": shop_domain,
            "last_success_at": "",
            "last_failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "fail_count": 1,
            "disputes_fetched": 0,
            "status_changed": 0,
            "api_status": "ERROR",
            "error_message": err_msg,
            "error_explanation": explanation
        }
        return parsed_disputes, sync_status, error_logs

    url = f"https://{shop_domain}/admin/api/{API_VERSION}/shopify_payments/disputes.json?limit=250"
    headers = {"X-Shopify-Access-Token": access_token}
    
    try:
        response = safe_request(session, "GET", url, headers=headers, timeout=20)
        
        if not response or response.status_code != 200:
            status_code = response.status_code if response else 0
            err_details = response.text[:200] if response else "安全重试 5 次后网络连接仍中断"
            explanation = translate_error("HTTPError", status_code, err_details)
            error_logs.append({
                "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "shop_name": shop_name,
                "shop_domain": shop_domain,
                "api": "REST disputes",
                "http_status": status_code,
                "error_type": "HTTPError",
                "error_details": err_details,
                "retry_count": 5,
                "error_explanation": explanation
            })
        else:
            disputes = response.json().get("disputes", [])
            for d in disputes:
                dispute_id = str(d["id"])
                raw_type = str(d.get("type", "CHARGEBACK")).upper()
                raw_status = str(d.get("status", "")).upper()
                raw_reason = str(d.get("reason", "")).lower()

                type_cn = TYPE_MAP.get(raw_type, raw_type)
                status_cn = STATUS_MAP.get(raw_status, raw_status)
                
                # N 列保持原始的详细拒付原因描述
                reason_cn = REASON_MAP.get(raw_reason, raw_reason if raw_reason else "未知原因")
                
                evidence_due_date = d.get("evidence_due_by", "") or "无"
                unique_key = f"{shop_domain}|{raw_type}|{dispute_id}"
                order_id = d.get("order_id")
                
                (order_name, order_total, cust_name, cust_email, 
                 refunds, order_tags, carrier, tracking_number) = get_order_details(
                    session, shop_domain, access_token, order_id, 
                    evidence_due_date=evidence_due_date, raw_reason=raw_reason
                 )
                
                refund_count = len(refunds)
                is_refunded = "是" if refund_count > 0 else "否"
                total_refunded = 0.0
                last_refund_at = ""
                for ref in refunds:
                    for line_item in ref.get("refund_line_items", []):
                        total_refunded += float(line_item.get("subtotal", 0.0))
                    ref_time = ref.get("created_at", "")
                    if ref_time > last_refund_at:
                        last_refund_at = ref_time
                        
                is_partially_refunded = "是" if (0 < total_refunded < order_total) else "否"
                needs_response = "是" if raw_status == "NEEDS_RESPONSE" else "否"
                
                parsed_disputes.append({
                    "key": unique_key,
                    "shop_name": shop_name,
                    "shop_domain": shop_domain,
                    "order_name": order_name,
                    "order_amount": order_total,
                    "dispute_id": dispute_id,
                    "type": type_cn,
                    "current_status": status_cn,
                    "created_at": d.get("initiated_at", ""),
                    "amount": str(d.get("amount", "0")),
                    "currency": d.get("currency", "USD"),
                    "reason": reason_cn,  # N列数据：保留原始详细描述
                    "customer_name": cust_name,
                    "customer_email": cust_email,
                    "evidence_due_date": evidence_due_date,
                    "needs_response": needs_response,
                    "is_refunded": is_refunded,
                    "is_partially_refunded": is_partially_refunded,
                    "refunded_amount": total_refunded,
                    "last_refund_at": last_refund_at,
                    "refund_count": refund_count,
                    "order_tags": order_tags,
                    "carrier": carrier,
                    "tracking_number": tracking_number
                })
    except Exception as e:
        err_details = str(e)[:200]
        explanation = translate_error(type(e).__name__, 0, err_details)
        error_logs.append({
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "shop_name": shop_name,
            "shop_domain": shop_domain,
            "api": "Network Exception",
            "http_status": 0,
            "error_type": type(e).__name__,
            "error_details": err_details,
            "retry_count": 5,
            "error_explanation": explanation
        })

    explanation_str = error_logs[0]["error_explanation"] if error_logs else ""
    sync_status = {
        "shop_name": shop_name,
        "shop_domain": shop_domain,
        "last_success_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if not error_logs else "",
        "last_failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if error_logs else "",
        "fail_count": 1 if error_logs else 0,
        "disputes_fetched": len(parsed_disputes),
        "status_changed": 0,
        "api_status": "ERROR" if error_logs else "NORMAL",
        "error_message": error_logs[0]["error_details"] if error_logs else "",
        "error_explanation": explanation_str
    }
    
    return parsed_disputes, sync_status, error_logs

# ==================== 分批推送 GAS 防超时引擎（强力抗锁与校验增强版） ====================
def post_to_gas(session, action, item_key, item_list, batch_size=30, max_retries=6):
    if not item_list:
        return
        
    headers = {"Content-Type": "application/json"}
    total = len(item_list)
    
    for i in range(0, total, batch_size):
        chunk = item_list[i:i + batch_size]
        payload = {"action": action, item_key: chunk}
        success = False
        
        for attempt in range(1, max_retries + 1):
            res = safe_request(session, "POST", GAS_WEBHOOK_URL, json=payload, headers=headers, timeout=60)
            if res and res.status_code == 200:
                try:
                    res_json = res.json()  # 严格校验返回内容是否为合法 JSON
                    
                    if res_json.get("status") == "success":
                        print(f"[{action}] 批次 ({i+1}-{min(i+batch_size, total)}/{total}) 推送成功: {res.text}")
                        success = True
                        break
                    elif res_json.get("status") == "error" and "Lock timeout" in res_json.get("message", ""):
                        wait = 3 * (2 ** (attempt - 1))  # 指数退避：3s, 6s, 12s, 24s...
                        print(f"[{action}] GAS 服务繁忙 (Lock timeout)，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                        time.sleep(wait)
                        continue
                    else:
                        wait = 3 * attempt
                        print(f"[{action}] GAS 返回了不符合预期的响应: {res.text[:100]}，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                        time.sleep(wait)
                        continue
                except Exception:
                    # 捕获 HTML 错误页（如 doGet 异常或 302 自动重定向降级后的 HTML 响应）
                    wait = 3 * attempt
                    print(f"[{action}] GAS 返回了非 JSON 响应(疑似重定向或 HTML 错误页)，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                    time.sleep(wait)
                    continue
            else:
                wait = 3 * attempt
                print(f"[{action}] 推送异常 (Status: {res.status_code if res else 'None'})，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                time.sleep(wait)
                
        if not success:
            print(f"❌ [{action}] 批次 ({i+1}-{min(i+batch_size, total)}/{total}) 经过 {max_retries} 次重试后仍然失败，请检查 GAS 后端限制！")
            
        time.sleep(1)  # 批次间加入 1 秒缓冲，极大降低 GAS 并发锁冲突

# ==================== 主流程控制 ====================
def main():
    if not os.path.exists(STORES_JSON_PATH):
        print(f"错误：找不到店铺配置文件 {STORES_JSON_PATH}")
        return

    with open(STORES_JSON_PATH, "r", encoding="utf-8") as f:
        stores_config = json.load(f)

    print(f"[{datetime.now()}] 成功加载店铺配置: {STORES_JSON_PATH}")
    print(f"开始同步 {len(stores_config)} 家店铺...")
    
    token_cache = load_token_cache()
    all_disputes = []
    all_sync_statuses = []
    all_error_logs = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_store = {executor.submit(fetch_disputes_for_shop, store, token_cache): store for store in stores_config}
        for future in as_completed(future_to_store):
            disputes, sync_status, error_logs = future.result()
            all_disputes.extend(disputes)
            all_sync_statuses.append(sync_status)
            all_error_logs.extend(error_logs)

    print(f"抓取结束：累计提取 {len(all_disputes)} 条拒付/查询案件，异常记录 {len(all_error_logs)} 条。")

    session = get_session()
    
    if all_disputes:
        post_to_gas(session, "SYNC_DISPUTES", "disputes", all_disputes, batch_size=30)
        
    if all_sync_statuses:
        post_to_gas(session, "UPDATE_SYNC_STATUS", "sync_status", all_sync_statuses, batch_size=50)
        
    if all_error_logs:
        post_to_gas(session, "LOG_ERROR", "error_logs", all_error_logs, batch_size=50)

if __name__ == "__main__":
    main()
