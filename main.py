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

# 核心性能配置
MAX_STORE_WORKERS = 10  # 店铺级并发数（从 3 提升至 10）
MAX_ORDER_WORKERS = 4   # 单店内部订单详情并发数

FILE_LOCK = threading.Lock()
# 本次截图对应的案件：即使 REST 与 GraphQL 返回相同，也记录两边原始原因便于核验。
REASON_AUDIT_TARGET_IDS = {"15997534549"}

DISPUTE_TAG_PATTERN = re.compile(r'^(\d{1,2}\.\d{1,2})(?:截止)?-(.+)$')

SHORT_REASON_MAP = {
    "fraudulent": "欺诈",
    "product_not_received": "未收到产品",
    "product_unacceptable": "产品不可接受",
    "unrecognized": "账单无法识别",
    "credit_not_processed": "抵扣额未处理",
    "duplicate": "重复付款",
    "general": "通用",
    "subscription_canceled": "订阅已取消",
    "canceled": "订单已取消",
    "bank_not_process": "银行无法处理",
    "customer_initiated": "客户主动发起",
    "debit_not_authorized": "扣款未获授权",
    "incorrect_account_details": "账户信息不正确",
    "insufficient_funds": "资金不足",
    "noncompliant": "不符合卡组织规则",
}

REASON_MAP = {
    "fraudulent": "欺诈/未授权交易",
    "product_not_received": "未收到货物",
    "product_unacceptable": "货不对板/商品缺陷",
    "unrecognized": "账单无法识别",
    "credit_not_processed": "抵扣额未处理",
    "duplicate": "重复扣款",
    "general": "通用",
    "subscription_canceled": "订阅已被取消",
    "canceled": "订单已取消",
    "bank_not_process": "银行无法处理此笔交易",
    "customer_initiated": "客户主动发起争议",
    "debit_not_authorized": "扣款未获授权",
    "incorrect_account_details": "账户详细信息不正确",
    "insufficient_funds": "账户资金不足",
    "noncompliant": "不符合卡组织规则",
}

# GraphQL and REST use slightly different enum spellings for a few reasons.
def normalize_reason_code(value):
    value = str(value or "").strip().lower()
    aliases = {
        "bank_cannot_process": "bank_not_process",
        "subscription_cancelled": "subscription_canceled",
    }
    return aliases.get(value, value)


def fetch_graphql_dispute_reasons(session, shop_domain, access_token):
    """Fetch canonical dispute reasons from GraphQL, keyed by REST legacyResourceId.

    Fail closed: if GraphQL is unavailable or does not cover all REST cases, caller
    must not sync possibly incorrect REST reasons into the sheet or order tags.
    """
    endpoint = f"https://{shop_domain}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    query = """
    query DisputeReasonAudit($first: Int!, $after: String) {
      disputes(first: $first, after: $after) {
        edges {
          cursor
          node {
            legacyResourceId
            reasonDetails {
              reason
              networkReasonCode
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """
    result = {}
    cursor = None
    while True:
        response = safe_request(
            session, "POST", endpoint, headers=headers,
            json={"query": query, "variables": {"first": 250, "after": cursor}},
            timeout=(10, 45),
        )
        if not response or response.status_code != 200:
            status = response.status_code if response else 0
            body = response.text[:500] if response else "无响应"
            scope_hint = "；请确认该应用已获 read_shopify_payments_disputes 权限并重新授权" if status in (401, 403) else ""
            raise RuntimeError(f"GraphQL 拒付原因查询失败 HTTP {status}: {body}{scope_hint}")

        try:
            payload = response.json()
        except Exception as exc:
            raise RuntimeError(f"GraphQL 返回非 JSON 响应: {exc}")

        if payload.get("errors"):
            error_text = json.dumps(payload["errors"], ensure_ascii=False)[:700]
            raise RuntimeError(
                "GraphQL 拒付原因查询返回 errors: " + error_text +
                "；请检查 read_shopify_payments_disputes API 权限、重新授权及 API 版本"
            )

        connection = (payload.get("data") or {}).get("disputes")
        if not isinstance(connection, dict):
            raise RuntimeError("GraphQL 响应缺少 data.disputes，不能验证案件原因")

        for edge in connection.get("edges") or []:
            node = edge.get("node") or {}
            legacy_id = str(node.get("legacyResourceId") or "").strip()
            reason_details = node.get("reasonDetails") or {}
            reason = normalize_reason_code(reason_details.get("reason"))
            if legacy_id and reason:
                result[legacy_id] = {
                    "reason": reason,
                    "network_reason_code": str(reason_details.get("networkReasonCode") or "").strip(),
                }

        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        next_cursor = page_info.get("endCursor")
        if not next_cursor or next_cursor == cursor:
            raise RuntimeError("GraphQL 分页异常：hasNextPage=true，但 endCursor 缺失或未变化")
        cursor = next_cursor

    if not result:
        raise RuntimeError("GraphQL 未返回任何拒付原因，拒绝使用未经核验的 REST reason")
    return result


def log_reason_mismatch(shop_name, shop_domain, dispute_item, rest_reason, graphql_reason, network_code):
    """Print and persist REST-vs-GraphQL reason discrepancies for later diagnosis."""
    entry = {
        "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "shop_name": shop_name,
        "shop_domain": shop_domain,
        "dispute_id": str(dispute_item.get("id", "")),
        "order_id": str(dispute_item.get("order_id", "")),
        "rest_reason": rest_reason,
        "graphql_reason": graphql_reason,
        "network_reason_code": network_code,
        "selected_reason": graphql_reason,
    }
    print("⚠️ [拒付原因源不一致] " + json.dumps(entry, ensure_ascii=False))
    try:
        with FILE_LOCK:
            with open("dispute_reason_audit.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"⚠️ 原因差异日志写入失败: {exc}")

TYPE_MAP = {"CHARGEBACK": "拒付", "INQUIRY": "调单/问询"}
STATUS_MAP = {
    "NEEDS_RESPONSE": "待回复", "UNDER_REVIEW": "审查中",
    "WON": "申诉成功(胜诉)", "LOST": "申诉失败(败诉)",
    "ACCEPTED": "已接受(放弃申诉)", "CHARGE_REFUNDED": "已全额退款"
}

def get_session():
    session = requests.Session()
    # 增加连接池容量以适应更高并发
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    return session

def safe_request(session, method, url, max_retries=5, **kwargs):
    kwargs.setdefault("timeout", (10, 30))
    
    headers = kwargs.get("headers", {})
    headers.setdefault("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    headers.setdefault("Accept", "application/json")
    kwargs["headers"] = headers

    last_resp = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(method, url, **kwargs)
            last_resp = resp
            
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "2")
                try:
                    wait_time = float(retry_after)
                except ValueError:
                    wait_time = 2.0
                time.sleep(wait_time + attempt * 0.5)
                continue
            elif resp.status_code in [500, 502, 503, 504]:
                time.sleep(2 * attempt)
                continue
                
            return resp
        except (requests.exceptions.RequestException, Exception) as e:
            if attempt == max_retries:
                raise e
            time.sleep(2 * attempt)
            
    return last_resp

def translate_error(error_type, http_status, error_details):
    details_str = str(error_details).lower()
    if http_status == 429 or "exceeded" in details_str or "rate limit" in details_str:
        return "【API限流 (429)】请求频率超出Shopify限制，已自动退避重试。"
    elif http_status == 401 or "token" in details_str or "autherror" in str(error_type).lower():
        return "【密钥认证失败 (401)】Client ID / Secret 填写错误或 Token 已失效。"
    elif http_status == 403 or "forbidden" in details_str:
        return "【无接口权限 (403)】当前 App 密钥缺乏 Shopify Payments 或 Orders 的读取权限。"
    elif http_status == 404:
        return "【接口不存在 (404)】该店铺可能未开通 Shopify Payments 或域名无效。"
    elif "ssleoferror" in details_str or "ssl" in details_str or "eof" in details_str:
        return "【SSL连接中断】网络抖动或并发过高被 Shopify 防火墙切断，脚本已自动重试。"
    elif "field" in details_str and "doesn't exist" in details_str:
        return "【API语法错误】请求中包含了当前 API 版本已废弃或不存在的字段。"
    elif http_status >= 500:
        return f"【Shopify服务端故障 ({http_status})】Shopify 官方服务器暂时崩溃，稍后重试即可。"
    else:
        return f"【网络/未知异常】{error_type} (HTTP {http_status}): {str(error_details)[:120]}"

def parse_dispute_date(evidence_due_date):
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

def process_order_tags_for_disputes(tags_list, dispute_items):
    """按订单汇总所有拒付案件标签，避免同一订单的不同原因互相覆盖。"""
    other_tags = []
    suffixes_by_base = {}
    all_suffixes = []

    for tag in tags_list:
        match = DISPUTE_TAG_PATTERN.match(str(tag).strip())
        if not match:
            other_tags.append(tag)
            continue

        date_part = match.group(1)
        rest_parts = match.group(2).split("-")
        # 标签原因本身采用现有中文短原因，不含连字符；其余部分为邮件等流程后缀。
        base_tag = f"{date_part}-{rest_parts[0].strip()}"
        suffixes = [s.strip() for s in rest_parts[1:] if s.strip()]
        suffixes_by_base.setdefault(base_tag, [])
        for suffix in suffixes:
            if suffix not in suffixes_by_base[base_tag]:
                suffixes_by_base[base_tag].append(suffix)
            if suffix not in all_suffixes:
                all_suffixes.append(suffix)

    # 使用每个案件自己的截止日期和原始 reason，生成去重且顺序稳定的标签。
    base_tags = []
    for item in sorted(dispute_items, key=lambda x: str(x.get("_initiated_at", "")), reverse=True):
        date_part = parse_dispute_date(item.get("_evidence_due_date", ""))
        raw_reason = str(item.get("_raw_reason", "")).lower()
        reason_short = SHORT_REASON_MAP.get(raw_reason, "其他")
        base_tag = f"{date_part}-{reason_short}" if date_part else reason_short
        if base_tag not in base_tags:
            base_tags.append(base_tag)

    if not base_tags:
        return other_tags

    result_dispute_tags = []
    used_suffixes = set()

    for base_tag in base_tags:
        matching_suffixes = suffixes_by_base.get(base_tag, [])
        if matching_suffixes:
            used_suffixes.update(matching_suffixes)
            result_dispute_tags.append(base_tag + "-" + "-".join(matching_suffixes))
        else:
            result_dispute_tags.append(base_tag)

    # 单案件时保持旧标签行为；多案件时将无法可靠归属到某一案件的旧后缀独立保留，
    # 不再把其他案件的后缀拼到某个拒付原因标签上。
    if len(base_tags) == 1:
        for suffix in all_suffixes:
            if suffix not in used_suffixes:
                result_dispute_tags[0] += "-" + suffix
                used_suffixes.add(suffix)
    else:
        for suffix in all_suffixes:
            if suffix not in used_suffixes and suffix not in other_tags and suffix not in result_dispute_tags:
                other_tags.append(suffix)

    return other_tags + result_dispute_tags


def sync_order_tags_for_disputes(session, shop_name, shop_domain, access_token, disputes):
    """按店铺+订单聚合案件，只对每个订单写回一次标签。"""
    grouped = {}
    for item in disputes:
        order_id = item.get("_order_id")
        if order_id:
            grouped.setdefault(str(order_id), []).append(item)

    for order_id, items in grouped.items():
        url = f"https://{shop_domain}/admin/api/{API_VERSION}/orders/{order_id}.json?fields=id,name,tags"
        headers = {"X-Shopify-Access-Token": access_token}
        try:
            response = safe_request(session, "GET", url, headers=headers)
            if not response or response.status_code != 200:
                status_code = response.status_code if response else "无响应"
                print(f"⚠️ [{shop_name}] 订单 {order_id} 读取标签失败，HTTP {status_code}；跳过标签写回")
                continue

            order_data = response.json().get("order", {})
            order_name = order_data.get("name", items[0].get("order_name", "N/A"))
            raw_tags = order_data.get("tags", "") or ""
            tags_list = [tag.strip() for tag in raw_tags.split(",") if tag.strip()]
            new_tags_list = process_order_tags_for_disputes(tags_list, items)

            if set(new_tags_list) != set(tags_list):
                update_url = f"https://{shop_domain}/admin/api/{API_VERSION}/orders/{order_id}.json"
                update_headers = {
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json",
                }
                payload = {"order": {"id": order_id, "tags": ", ".join(new_tags_list)}}
                update_response = safe_request(
                    session, "PUT", update_url, headers=update_headers, json=payload
                )
                if not update_response or update_response.status_code != 200:
                    status_code = update_response.status_code if update_response else "无响应"
                    print(f"⚠️ [{shop_name}] 订单 {order_name} 标签写回失败，HTTP {status_code}")
                    new_tags_list = tags_list
                else:
                    print(
                        f"***{shop_name}*** 订单 {order_name} 标签已按案件汇总更新为: "
                        f"{', '.join(new_tags_list)}"
                    )
            else:
                # Shopify 上的标签集合已正确，无需为顺序差异重复写回。
                new_tags_list = tags_list

            formatted_tags = "\n".join(new_tags_list)
            for item in items:
                item["order_tags"] = formatted_tags
                print(
                    f"[拒付原因核对] 店铺={shop_name} 案件ID={item.get('dispute_id')} "
                    f"订单={order_name} 原因码={item.get('_raw_reason')!r} "
                    f"表格原因={item.get('reason')!r}"
                )
        except Exception as exc:
            print(f"⚠️ [{shop_name}] 订单 {order_id} 标签汇总更新异常: {exc}")

    # 仅保留对 GAS 有用的业务字段，内部排错字段不推送。
    for item in disputes:
        item.pop("_order_id", None)
        item.pop("_raw_reason", None)
        item.pop("_evidence_due_date", None)
        item.pop("_initiated_at", None)


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
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)
            with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"写入 Token 缓存文件失败: {e}")

def get_access_token(session, store, cache):
    domain = store["domain"]
    client_id = store["client_id"]
    client_secret = store["client_secret"]
    shop_name = store.get("name", domain)

    # Client Credentials access tokens expire after expires_in (normally 24 hours).
    # Old cache entries without expires_at are intentionally refreshed once.
    cached = cache.get(domain) or {}
    cached_token = cached.get("access_token")
    expires_at = float(cached.get("expires_at", 0) or 0)
    if cached_token and expires_at > time.time() + 300:
        return cached_token

    token_url = f"https://{domain}/admin/oauth/access_token"
    payload = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}
    
    try:
        resp = safe_request(session, "POST", token_url, json=payload)
        if resp and resp.status_code == 200:
            token_data = resp.json()
            access_token = token_data.get("access_token")
            if access_token:
                expires_in = int(token_data.get("expires_in", 86399) or 86399)
                cache[domain] = {
                    "access_token": access_token,
                    "expires_at": time.time() + expires_in,
                }
                save_token_cache(cache)
                return access_token
    except Exception as e:
        print(f"[{shop_name}] 请求 Token 异常: {e}")
        
    return None

def get_order_details(session, shop_name, shop_domain, access_token, order_id, evidence_due_date="", raw_reason=""):
    """只读取订单信息，不在并发案件线程里修改订单标签。"""
    if not order_id:
        return "N/A", 0.0, "N/A", "N/A", [], "", "无", "无"

    url = f"https://{shop_domain}/admin/api/{API_VERSION}/orders/{order_id}.json?fields=id,name,total_price,total_price_set,customer,refunds,tags,fulfillments"
    headers = {"X-Shopify-Access-Token": access_token}
    try:
        res = safe_request(session, "GET", url, headers=headers)
        if res and res.status_code == 200:
            ord_data = res.json().get("order", {})
            order_name = ord_data.get("name", "N/A")

            price_set = ord_data.get("total_price_set") or {}
            presentment_money = price_set.get("presentment_money") or {}
            if "amount" in presentment_money and presentment_money.get("amount") is not None:
                order_total = float(presentment_money.get("amount", 0.0))
            else:
                order_total = float(ord_data.get("total_price", 0.0))

            cust = ord_data.get("customer") or {}
            cust_name = f"{cust.get('first_name', '')} {cust.get('last_name', '')}".strip() or "N/A"
            cust_email = cust.get("email", "") or "N/A"
            refunds = ord_data.get("refunds", [])

            raw_tags = ord_data.get("tags", "") or ""
            tags_list = [tag.strip() for tag in raw_tags.split(",") if tag.strip()]
            formatted_tags = "\n".join(tags_list)

            fulfillments = ord_data.get("fulfillments", [])
            carriers = []
            tracking_numbers = []
            for fulfillment in fulfillments:
                company = fulfillment.get("tracking_company")
                if company and str(company).strip() not in carriers:
                    carriers.append(str(company).strip())

                tracking_number = fulfillment.get("tracking_number")
                if tracking_number and str(tracking_number).strip() not in tracking_numbers:
                    tracking_numbers.append(str(tracking_number).strip())
                for number in fulfillment.get("tracking_numbers") or []:
                    number_str = str(number).strip()
                    if number_str and number_str not in tracking_numbers:
                        tracking_numbers.append(number_str)

            carrier_str = "\n".join(carriers) if carriers else "无"
            tracking_str = "\n".join(tracking_numbers) if tracking_numbers else "无"
            return (order_name, order_total, cust_name, cust_email, refunds,
                    formatted_tags, carrier_str, tracking_str)

        if res:
            print(f"⚠️ [{shop_name}] 获取订单 {order_id} 详情失败，HTTP {res.status_code}: {res.text[:200]}")
    except Exception as exc:
        print(f"[{shop_name}] 获取订单 {order_id} 详情异常: {exc}")

    return "N/A", 0.0, "N/A", "N/A", [], "", "无", "无"

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
    
    raw_dispute_items = []
    try:
        while url:
            response = safe_request(session, "GET", url, headers=headers)
            
            if not response or response.status_code != 200:
                status_code = response.status_code if response else 0
                err_details = response.text[:200] if response else "网络连接超时或响应为空"
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
                break
            else:
                disputes = response.json().get("disputes", [])
                raw_dispute_items.extend(disputes)

                link_header = response.headers.get("Link", "")
                next_url = None
                if link_header:
                    links = link_header.split(",")
                    for link in links:
                        if 'rel="next"' in link:
                            next_url = link.split(";")[0].strip("<> ")
                            break
                url = next_url

        # 用 GraphQL reasonDetails.reason 交叉核验 REST 列表 reason。
        # 任一案件无法由 GraphQL 对上时，整个店铺本轮停止写入，避免把错因继续传播。
        if raw_dispute_items:
            graphql_reasons = fetch_graphql_dispute_reasons(session, shop_domain, access_token)
            missing_ids = [str(item.get("id", "")) for item in raw_dispute_items
                           if str(item.get("id", "")) not in graphql_reasons]
            missing_reasons = [str(item.get("id", "")) for item in raw_dispute_items
                               if str(item.get("id", "")) in graphql_reasons
                               and not graphql_reasons[str(item.get("id", ""))].get("reason")]
            if missing_ids or missing_reasons:
                sample = (missing_ids + missing_reasons)[:20]
                raise RuntimeError(
                    "GraphQL 拒付案件与 REST 列表未能完整匹配；为避免错误原因写入，本店本轮不做同步。"
                    f"缺失/无原因案件ID示例: {sample}。请确认 GraphQL 权限及 API 数据完整性。"
                )

            for dispute_item in raw_dispute_items:
                dispute_id_for_reason = str(dispute_item.get("id", ""))
                rest_reason = normalize_reason_code(dispute_item.get("reason"))
                canonical = graphql_reasons[dispute_id_for_reason]
                graphql_reason = canonical["reason"]
                network_code = canonical.get("network_reason_code", "")
                if (rest_reason and rest_reason != graphql_reason) or dispute_id_for_reason in REASON_AUDIT_TARGET_IDS:
                    log_reason_mismatch(
                        shop_name, shop_domain, dispute_item,
                        rest_reason, graphql_reason, network_code
                    )
                dispute_item["_rest_reason"] = rest_reason
                dispute_item["reason"] = graphql_reason
                dispute_item["network_reason_code"] = network_code

        # 店铺内部订单详情抓取引入并发池优化
        if raw_dispute_items:
            def process_single_dispute(d):
                dispute_id = str(d["id"])
                raw_type = str(d.get("type", "CHARGEBACK")).upper()
                raw_status = str(d.get("status", "")).upper()
                raw_reason = normalize_reason_code(d.get("reason", ""))

                type_cn = TYPE_MAP.get(raw_type, raw_type)
                status_cn = STATUS_MAP.get(raw_status, raw_status)
                reason_cn = REASON_MAP.get(raw_reason, raw_reason if raw_reason else "未知原因")
                
                evidence_due_date = d.get("evidence_due_by", "") or "无"
                unique_key = f"{shop_domain}|{dispute_id}"
                order_id = d.get("order_id")
                
                (order_name, order_total, cust_name, cust_email, 
                 refunds, order_tags, carrier, tracking_number) = get_order_details(
                    session, shop_name, shop_domain, access_token, order_id, 
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
                
                return {
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
                    "reason": reason_cn,
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
                    "tracking_number": tracking_number,
                    "_order_id": order_id,
                    "_raw_reason": raw_reason,
                    "_network_reason_code": str(d.get("network_reason_code", "") or ""),
                    "_rest_reason": str(d.get("_rest_reason", "") or ""),
                    "_evidence_due_date": evidence_due_date,
                    "_initiated_at": d.get("initiated_at", "")
                }

            with ThreadPoolExecutor(max_workers=MAX_ORDER_WORKERS) as order_executor:
                futures = [order_executor.submit(process_single_dispute, item) for item in raw_dispute_items]
                for future in as_completed(futures):
                    try:
                        parsed_disputes.append(future.result())
                    except Exception as sub_e:
                        print(f"[{shop_name}] 处理单条拒付解析异常: {sub_e}")

            # 先独立解析每条案件，再按订单聚合标签；避免同订单多个案件并发覆盖。
            sync_order_tags_for_disputes(session, shop_name, shop_domain, access_token, parsed_disputes)

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

def post_to_gas(session, action, item_key, item_list, batch_size=500, max_retries=6):  # 🚀 批次大小提升至 500
    if not item_list:
        return
        
    headers = {"Content-Type": "application/json"}
    total = len(item_list)
    
    for i in range(0, total, batch_size):
        chunk = item_list[i:i + batch_size]
        payload = {"action": action, item_key: chunk}
        success = False
        
        for attempt in range(1, max_retries + 1):
            # 🚀 单批次变大，适当放宽超时时间到 90 秒
            res = safe_request(session, "POST", GAS_WEBHOOK_URL, json=payload, headers=headers, timeout=(10, 90))
            if res and res.status_code == 200:
                try:
                    res_json = res.json()
                    if res_json.get("status") == "success":
                        print(f"[{action}] 批次 ({i+1}-{min(i+batch_size, total)}/{total}) 推送成功")
                        success = True
                        break
                    elif res_json.get("status") == "error" and "Lock timeout" in res_json.get("message", ""):
                        wait = 3 * (2 ** (attempt - 1))
                        print(f"[{action}] GAS 服务繁忙 (Lock timeout)，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                        time.sleep(wait)
                        continue
                    else:
                        wait = 3 * attempt
                        print(f"[{action}] GAS 返回了不符合预期的响应: {res.text[:100]}，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                        time.sleep(wait)
                        continue
                except Exception:
                    wait = 3 * attempt
                    print(f"[{action}] GAS 返回了非 JSON 响应，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                    time.sleep(wait)
                    continue
            else:
                wait = 3 * attempt
                print(f"[{action}] 推送异常 (Status: {res.status_code if res else 'None'})，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                time.sleep(wait)
                
        if not success:
            print(f"❌ [{action}] 批次 ({i+1}-{min(i+batch_size, total)}/{total}) 经过 {max_retries} 次重试后仍然失败，请检查 GAS 后端限制！")
            
        time.sleep(0.1)  # 🚀 将批次间歇缩短至 0.1 秒，大幅提速

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
    
    with ThreadPoolExecutor(max_workers=MAX_STORE_WORKERS) as executor:
        future_to_store = {executor.submit(fetch_disputes_for_shop, store, token_cache): store for store in stores_config}
        for future in as_completed(future_to_store):
            disputes, sync_status, error_logs = future.result()
            all_disputes.extend(disputes)
            all_sync_statuses.append(sync_status)
            all_error_logs.extend(error_logs)

    print(f"抓取结束：累计提取 {len(all_disputes)} 条拒付/查询案件，异常记录 {len(all_error_logs)} 条。")

    session = get_session()
    
    if all_disputes:
        post_to_gas(session, "SYNC_DISPUTES", "disputes", all_disputes, batch_size=500)
        
    if all_sync_statuses:
        post_to_gas(session, "UPDATE_SYNC_STATUS", "sync_status", all_sync_statuses, batch_size=500)
        
    if all_error_logs:
        post_to_gas(session, "LOG_ERROR", "error_logs", all_error_logs, batch_size=500)

if __name__ == "__main__":
    main()
