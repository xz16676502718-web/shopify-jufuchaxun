import os
import re
import json
import html
import time
import threading
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse

# ==================== 配置区 ====================
STORES_JSON_PATH = "stores.json"
TOKEN_CACHE_FILE = "token_cache.json"

# WebApp 部署地址
GAS_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbz5lkaskmNJRA_X0iL_QQyRabeLzmnWmtuTETu3TrUEew7UOXzSd-ccas2yK0h68GE/exec"
API_VERSION = "2026-10"

# 核心性能配置
MAX_STORE_WORKERS = 10  # 店铺级并发数（从 3 提升至 10）
MAX_ORDER_WORKERS = 4   # 单店内部订单详情并发数

FILE_LOCK = threading.Lock()
TOKEN_FAILURES_LOCK = threading.Lock()
TOKEN_FAILURES = {}  # domain -> actual OAuth failure details for the current run
COUNTER_LOCK = threading.Lock()
AUDIT_COUNTERS = {
    "reason_source_mismatch": 0,
    "status_source_mismatch": 0,
    "inquiry_lost_review": 0,
    "admin_alert_status_override": 0,
    "order_detail_read_failure": 0,
    "tag_write_failure": 0,
    "tag_write_skipped_no_snapshot": 0,
    "stores_missing_read_all_orders": 0,
    "stores_missing_read_orders": 0,
    "stores_scope_unknown": 0,
}

SCOPE_AUDIT_LOCK = threading.Lock()
SCOPE_AUDIT_RESULTS = {}

# Shopify REST Admin API default standard rate is 2 requests/second per app/store.
# Keep each store below that rate; stores are throttled independently, so other stores continue.
SHOPIFY_REST_MIN_INTERVAL = 0.55
SHOPIFY_RATE_LIMITERS = {}
SHOPIFY_RATE_LIMITERS_LOCK = threading.Lock()

# 特定案件：即使 REST 与 GraphQL 返回相同，也记录两边原始原因便于核验。
REASON_AUDIT_TARGET_IDS = {"15997534549"}


def increment_audit_counter(name, amount=1):
    with COUNTER_LOCK:
        AUDIT_COUNTERS[name] = AUDIT_COUNTERS.get(name, 0) + amount


def _get_shopify_rest_limiter(url):
    parsed = urlparse(url)
    if "/admin/api/" not in parsed.path or parsed.path.endswith("/graphql.json"):
        return None
    host = (parsed.hostname or "").lower()
    if not host.endswith("myshopify.com"):
        return None
    with SHOPIFY_RATE_LIMITERS_LOCK:
        limiter = SHOPIFY_RATE_LIMITERS.get(host)
        if limiter is None:
            limiter = {
                "lock": threading.Lock(),
                "next_allowed": 0.0,
                "blocked_until": 0.0,
            }
            SHOPIFY_RATE_LIMITERS[host] = limiter
    return limiter


def wait_for_shopify_rest_slot(url):
    limiter = _get_shopify_rest_limiter(url)
    if limiter is None:
        return
    with limiter["lock"]:
        now = time.monotonic()
        target = max(limiter["next_allowed"], limiter["blocked_until"])
        if target > now:
            time.sleep(target - now)
        limiter["next_allowed"] = time.monotonic() + SHOPIFY_REST_MIN_INTERVAL


def defer_shopify_rest_store(url, seconds):
    limiter = _get_shopify_rest_limiter(url)
    if limiter is None:
        return
    with limiter["lock"]:
        blocked_until = time.monotonic() + max(0.0, seconds)
        limiter["blocked_until"] = max(limiter["blocked_until"], blocked_until)
        limiter["next_allowed"] = max(limiter["next_allowed"], limiter["blocked_until"])

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


def _clean_shopify_alert_text(value):
    """Convert Shopify ResourceAlert HTML/text to readable plain text."""
    text = html.unescape(str(value or ""))
    text = re.sub(r"<\s*br\s*/?\s*>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"</\s*(p|div|li)\s*>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def select_shopify_admin_dispute_alert(raw_type, raw_status, alerts):
    """Return an exact Shopify Orders-page dispute alert only when clearly relevant.

    Order.alerts is a list of order-level messages, not a guaranteed one-to-one
    dispute status field. Therefore only explicit dispute-related alert text may
    override the enum translation; generic order alerts are ignored.
    """
    raw_type = str(raw_type or "").strip().upper()
    raw_status = str(raw_status or "").strip().upper()
    if raw_type != "CHARGEBACK":
        return "", ""

    dispute_terms = ("拒付", "争议", "chargeback", "dispute")
    explicit_withdrawal_terms = (
        "已被撤销", "拒付已撤销", "争议已撤销", "客户撤销",
        "dispute was withdrawn", "withdrawn the dispute",
        "chargeback was reversed", "reversed the chargeback",
        "withdrawn by the customer", "customer withdrew",
    )

    candidates = []
    for alert in alerts or []:
        if not isinstance(alert, dict):
            continue
        title = _clean_shopify_alert_text(alert.get("title", ""))
        content = _clean_shopify_alert_text(alert.get("content", ""))
        combined = " ".join(part for part in (title, content) if part).strip()
        lowered = combined.casefold()
        if not combined or not any(term in lowered for term in dispute_terms):
            continue
        candidates.append((title, content, combined, lowered))

    # WON alone is not proof of customer withdrawal. Require explicit withdrawal
    # language from Shopify's own order alert before using that exact wording.
    if raw_status == "WON":
        for title, content, combined, lowered in candidates:
            if any(term in lowered for term in explicit_withdrawal_terms):
                # Prefer Shopify's secondary alert text, which is the full sentence
                # shown in the green banner; fall back to its title if content is empty.
                return (content or title or combined), "ORDER_ALERT_EXPLICIT_WITHDRAWAL"

    # For other cases, do not replace the API status with a generic order alert.
    return "", ""


def fetch_graphql_dispute_details(session, shop_domain, access_token):
    """Fetch canonical reason/type/status from GraphQL, keyed by REST legacyResourceId.

    GraphQL is treated as the authoritative source for dispute type and status.
    Fail closed if any REST dispute cannot be matched or lacks required fields.
    """
    endpoint = f"https://{shop_domain}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    query = """
    query DisputeAudit($first: Int!, $after: String) {
      disputes(first: $first, after: $after) {
        edges {
          cursor
          node {
            id
            legacyResourceId
            type
            status
            finalizedOn
            order {
              id
              alerts {
                title
                content
                severity
              }
            }
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
        if response is None or response.status_code != 200:
            status = response.status_code if response is not None else 0
            body = response.text[:500] if response is not None else "无响应"
            scope_hint = "；请确认该应用已获 read_shopify_payments_disputes 权限并重新授权" if status in (401, 403) else ""
            raise RuntimeError(f"GraphQL 拒付案件详情查询失败 HTTP {status}: {body}{scope_hint}")

        try:
            payload = response.json()
        except Exception as exc:
            raise RuntimeError(f"GraphQL 返回非 JSON 响应: {exc}")

        if payload.get("errors"):
            error_text = json.dumps(payload["errors"], ensure_ascii=False)[:700]
            raise RuntimeError(
                "GraphQL 拒付案件详情查询返回 errors: " + error_text +
                "；请检查 read_shopify_payments_disputes API 权限、重新授权及 API 版本"
            )

        connection = (payload.get("data") or {}).get("disputes")
        if not isinstance(connection, dict):
            raise RuntimeError("GraphQL 响应缺少 data.disputes，不能核验案件类型和状态")

        for edge in connection.get("edges") or []:
            node = edge.get("node") or {}
            legacy_id = str(node.get("legacyResourceId") or "").strip()
            dispute_gid = str(node.get("id") or "").strip()
            reason_details = node.get("reasonDetails") or {}
            reason = normalize_reason_code(reason_details.get("reason"))
            dispute_type = str(node.get("type") or "").strip().upper()
            dispute_status = str(node.get("status") or "").strip().upper()
            finalized_on = str(node.get("finalizedOn") or "").strip()
            order_node = node.get("order") or {}
            order_alerts = []
            for alert in order_node.get("alerts") or []:
                if not isinstance(alert, dict):
                    continue
                title = _clean_shopify_alert_text(alert.get("title", ""))
                content = _clean_shopify_alert_text(alert.get("content", ""))
                severity = str(alert.get("severity") or "").strip().upper()
                if title or content:
                    order_alerts.append({
                        "title": title,
                        "content": content,
                        "severity": severity,
                    })

            if legacy_id:
                result[legacy_id] = {
                    "reason": reason,
                    "network_reason_code": str(reason_details.get("networkReasonCode") or "").strip(),
                    "type": dispute_type,
                    "status": dispute_status,
                    "finalized_on": finalized_on,
                    "dispute_gid": dispute_gid,
                    "order_gid": str(order_node.get("id") or "").strip(),
                    "order_alerts": order_alerts,
                }

        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        next_cursor = page_info.get("endCursor")
        if not next_cursor or next_cursor == cursor:
            raise RuntimeError("GraphQL 分页异常：hasNextPage=true，但 endCursor 缺失或未变化")
        cursor = next_cursor

    if not result:
        raise RuntimeError("GraphQL 未返回任何拒付案件详情，拒绝使用未经核验的 REST 数据")
    return result


def log_reason_mismatch(shop_name, shop_domain, dispute_item, rest_reason, graphql_reason, network_code):
    """Save per-dispute REST-vs-GraphQL reason differences without flooding Actions logs."""
    increment_audit_counter("reason_source_mismatch")
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
    try:
        with FILE_LOCK:
            with open("dispute_reason_audit.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"⚠️ 原因差异日志写入失败: {exc}")


def log_dispute_status_audit(shop_name, shop_domain, dispute_item, rest_type,
                             rest_status, graphql_type, graphql_status, finalized_on):
    """Save only genuine source conflicts or the unusual Inquiry+LOST combination."""
    source_mismatch = (rest_type != graphql_type) or (rest_status != graphql_status)
    inquiry_lost = graphql_type == "INQUIRY" and graphql_status == "LOST"
    if not source_mismatch and not inquiry_lost:
        return

    if source_mismatch:
        increment_audit_counter("status_source_mismatch")
    if inquiry_lost:
        increment_audit_counter("inquiry_lost_review")

    entry = {
        "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "shop_name": shop_name,
        "shop_domain": shop_domain,
        "dispute_id": str(dispute_item.get("id", "")),
        "order_id": str(dispute_item.get("order_id", "")),
        "rest_type": rest_type,
        "rest_status": rest_status,
        "graphql_type": graphql_type,
        "graphql_status": graphql_status,
        "finalized_on": finalized_on,
        "selected_type": graphql_type,
        "selected_status": graphql_status,
        "inquiry_lost_needs_review": inquiry_lost,
    }
    try:
        with FILE_LOCK:
            with open("dispute_status_audit.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"⚠️ 案件状态核验日志写入失败: {exc}")

TYPE_MAP = {"CHARGEBACK": "拒付", "INQUIRY": "调单/问询"}

# Formal chargeback labels. INQUIRY is translated separately below so an inquiry
# can never be mislabeled as a successful/failed appeal merely because of enum reuse.
STATUS_MAP = {
    "NEEDS_RESPONSE": "待回复",
    "UNDER_REVIEW": "审查中",
    "WON": "拒付胜诉",
    "LOST": "申诉失败(败诉)",
    "ACCEPTED": "已接受(放弃申诉)",
    "CHARGE_REFUNDED": "争议款已退款",
    # Network-level prevention: the dispute was stopped before becoming a formal chargeback.
    "PREVENTED": "拒付已解决（自动阻止升级）",
}

INQUIRY_STATUS_MAP = {
    "NEEDS_RESPONSE": "查询未解决",
    "UNDER_REVIEW": "查询证据已提交",
    "CHARGE_REFUNDED": "查询已退款关闭",
    "ACCEPTED": "查询已接受并关闭",
    "WON": "查询已关闭（平台标记胜诉）",
    "PREVENTED": "查询已解决（阻止升级为拒付）",
    # An INQUIRY should not be called a lost chargeback. Keep the raw combination
    # visible as a review item until the merchant checks Shopify Admin.
    "LOST": "查询已关闭（状态需核实）",
}


def translate_dispute_status(raw_type, raw_status):
    raw_type = str(raw_type or "").strip().upper()
    raw_status = str(raw_status or "").strip().upper()

    if raw_type == "INQUIRY":
        return INQUIRY_STATUS_MAP.get(raw_status, f"查询状态待核实：{raw_status or '未知'}")

    return STATUS_MAP.get(raw_status, raw_status or "未知状态")


def get_session():
    session = requests.Session()
    # 增加连接池容量以适应更高并发
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    return session

def safe_request(session, method, url, max_retries=5, retry_delay=2, **kwargs):
    kwargs.setdefault("timeout", (10, 30))

    headers = kwargs.get("headers", {})
    headers.setdefault("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    headers.setdefault("Accept", "application/json")
    kwargs["headers"] = headers

    last_resp = None
    for attempt in range(1, max_retries + 1):
        wait_for_shopify_rest_slot(url)
        try:
            resp = session.request(method, url, **kwargs)
            last_resp = resp

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "2")
                try:
                    wait_time = max(0.0, float(retry_after))
                except (TypeError, ValueError):
                    wait_time = 2.0
                # Tell all concurrent requests for this same shop to wait too.
                defer_shopify_rest_store(url, wait_time + 0.1)
                if attempt < max_retries:
                    continue
                return resp

            if resp.status_code in (500, 502, 503, 504):
                if attempt < max_retries:
                    time.sleep(max(0.0, retry_delay) * attempt)
                    continue
                return resp

            return resp

        except requests.exceptions.RequestException:
            if attempt >= max_retries:
                raise
            time.sleep(max(0.0, retry_delay) * attempt)
        except Exception:
            if attempt >= max_retries:
                raise
            time.sleep(max(0.0, retry_delay) * attempt)

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


def _split_order_tags(raw_tags):
    """Normalize Shopify comma-separated tags or the sheet's newline format."""
    if not raw_tags:
        return []
    return [part.strip() for part in re.split(r"[,\n]+", str(raw_tags)) if part.strip()]


def sync_order_tags_for_disputes(session, shop_name, shop_domain, access_token, disputes):
    """Merge dispute tags using the order snapshot already fetched for enrichment.

    Avoid a second GET per order: it doubled REST traffic and contributed to 429s.
    If no order-detail snapshot exists, do not PUT potentially incomplete tags.
    """
    grouped = {}
    for item in disputes:
        order_id = item.get("_order_id")
        if order_id:
            grouped.setdefault(str(order_id), []).append(item)

    if not grouped:
        _strip_internal_dispute_fields(disputes)
        return

    def process_one_order(order_id, items):
        # Select the most recently completed successful snapshot for this order.
        loaded_items = [item for item in items if item.get("_order_tags_loaded")]
        if not loaded_items:
            increment_audit_counter("tag_write_skipped_no_snapshot")
            with FILE_LOCK:
                with open("shopify_tag_write_errors.jsonl", "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "shop_name": shop_name,
                        "shop_domain": shop_domain,
                        "order_id": str(order_id),
                        "action": "SKIP_PUT_NO_ORDER_SNAPSHOT",
                        "message": "订单详情未成功读取；为避免覆盖未知标签，不执行 Shopify 标签写回",
                    }, ensure_ascii=False) + "\n")
            return "skipped"

        snapshot = loaded_items[-1]
        order_name = snapshot.get("order_name", "N/A")
        tags_list = _split_order_tags(snapshot.get("order_tags", ""))

        # If multiple disputes share one order and one detail request failed,
        # reuse the successful same-order snapshot for all those dispute rows.
        transferable_fields = (
            "order_name", "order_amount", "customer_name", "customer_email",
            "is_refunded", "is_partially_refunded", "refunded_amount",
            "last_refund_at", "refund_count", "carrier", "tracking_number",
        )
        for item in items:
            if not item.get("_order_tags_loaded"):
                for field in transferable_fields:
                    item[field] = snapshot.get(field)
            item["order_details_loaded"] = True

        new_tags_list = process_order_tags_for_disputes(tags_list, items)
        final_tags_list = tags_list
        tags_changed = set(new_tags_list) != set(tags_list)

        if tags_changed:
            update_url = f"https://{shop_domain}/admin/api/{API_VERSION}/orders/{order_id}.json"
            update_headers = {
                "X-Shopify-Access-Token": access_token,
                "Content-Type": "application/json",
            }
            payload = {"order": {"id": order_id, "tags": ", ".join(new_tags_list)}}
            try:
                update_response = safe_request(
                    session, "PUT", update_url, headers=update_headers, json=payload,
                    max_retries=3, retry_delay=0.25, timeout=(5, 12)
                )
                update_error = ""
            except Exception as exc:
                update_response = None
                update_error = f"{type(exc).__name__}: {str(exc)[:160]}"

            if update_response is not None and update_response.status_code == 200:
                final_tags_list = new_tags_list
                write_failed = False
            else:
                increment_audit_counter("tag_write_failure")
                status_code = update_response.status_code if update_response is not None else "无HTTP响应"
                response_snippet = update_response.text[:160] if update_response is not None else ""
                with FILE_LOCK:
                    with open("shopify_tag_write_errors.jsonl", "a", encoding="utf-8") as f:
                        f.write(json.dumps({
                            "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            "shop_name": shop_name,
                            "shop_domain": shop_domain,
                            "order_id": str(order_id),
                            "order_name": order_name,
                            "action": "PUT_TAGS_FAILED",
                            "http_status": status_code,
                            "error": update_error,
                            "response": response_snippet,
                        }, ensure_ascii=False) + "\n")
                # Keep the sheet's order_tags field aligned with the last known Shopify snapshot,
                # not the unconfirmed value that failed to write back.
                final_tags_list = tags_list
                write_failed = True
        else:
            write_failed = False

        formatted_tags = "\n".join(final_tags_list)
        for item in items:
            item["order_tags"] = formatted_tags
            item["order_details_loaded"] = True

        if write_failed:
            return "write_failed"
        return "updated" if tags_changed else "unchanged"

    counts = {"updated": 0, "unchanged": 0, "write_failed": 0, "skipped": 0, "exception": 0}
    max_workers = min(MAX_ORDER_WORKERS, len(grouped))
    with ThreadPoolExecutor(max_workers=max_workers) as tag_executor:
        futures = [
            tag_executor.submit(process_one_order, order_id, items)
            for order_id, items in grouped.items()
        ]
        for future in as_completed(futures):
            try:
                result = future.result()
                counts[result] = counts.get(result, 0) + 1
            except Exception as exc:
                counts["exception"] += 1
                print(f"⚠️ [{shop_name}] 订单标签并发处理异常: {type(exc).__name__}: {exc}")

    if any(counts.values()):
        print(
            f"[{shop_name}] 标签同步汇总：订单 {len(grouped)} 个；"
            f"写回成功 {counts['updated']} 个；无需变更 {counts['unchanged']} 个；"
            f"写回失败 {counts['write_failed']} 个；缺少详情快照而安全跳过 {counts['skipped']} 个；"
            f"异常 {counts['exception']} 个。"
        )

    _strip_internal_dispute_fields(disputes)

def _strip_internal_dispute_fields(disputes):
    """Remove internal-only values before sending dispute rows to GAS."""
    internal_keys = (
        "_order_id", "_raw_reason", "_evidence_due_date", "_initiated_at",
        "_rest_type", "_rest_status", "_graphql_type", "_graphql_status",
        "_order_summary_type", "_order_summary_status", "_network_reason_code", "_rest_reason",
        "_finalized_on",
        "_successful_refund_transactions", "_order_tags_loaded",
    )
    for item in disputes:
        for key in internal_keys:
            item.pop(key, None)


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

def parse_shopify_scopes(scope_value):
    """Parse Shopify's token-response scope string into a normalized set."""
    if isinstance(scope_value, (list, tuple, set)):
        parts = [str(x).strip() for x in scope_value]
    else:
        parts = re.split(r"[,\s]+", str(scope_value or "").strip())
    return {part for part in parts if part}


def fetch_granted_scopes_fallback(session, shop_domain, access_token):
    """Fallback only when the token response omits its documented scope field."""
    url = f"https://{shop_domain}/admin/oauth/access_scopes.json"
    headers = {"X-Shopify-Access-Token": access_token}
    try:
        response = safe_request(
            session, "GET", url, headers=headers,
            max_retries=2, retry_delay=0.25, timeout=(5, 10)
        )
        if response is not None and response.status_code == 200:
            items = (response.json() or {}).get("access_scopes") or []
            return ",".join(
                str(item.get("handle") or "").strip()
                for item in items if item.get("handle")
            ), "access_scopes_endpoint"
        return "", f"access_scopes_http_{response.status_code if response is not None else 'no_response'}"
    except Exception as exc:
        return "", f"access_scopes_error_{type(exc).__name__}"


def record_shopify_scope_audit(shop_name, shop_domain, scope_value, source="token_response"):
    scopes = parse_shopify_scopes(scope_value)
    known = bool(scopes)
    has_read_orders = "read_orders" in scopes or "write_orders" in scopes
    has_read_all_orders = "read_all_orders" in scopes
    result = {
        "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "shop_name": shop_name,
        "shop_domain": shop_domain,
        "scope_source": source,
        "scope_known": known,
        "has_read_orders_or_write_orders": has_read_orders,
        "has_read_all_orders": has_read_all_orders,
        "can_read_orders_older_than_60_days": bool(has_read_orders and has_read_all_orders),
        "has_read_shopify_payments_disputes": ("read_shopify_payments_disputes" in scopes or "write_shopify_payments_disputes" in scopes),
        "granted_scope_count": len(scopes),
        "granted_scopes": sorted(scopes),
    }
    with SCOPE_AUDIT_LOCK:
        SCOPE_AUDIT_RESULTS[shop_domain] = result
    try:
        with FILE_LOCK:
            with open("shopify_scope_audit.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(result, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"⚠️ [{shop_name}] Shopify 权限诊断日志写入失败: {exc}")


def audit_cached_or_new_scope(session, shop_name, shop_domain, access_token, scope_value, source):
    if not str(scope_value or "").strip():
        fallback_value, fallback_source = fetch_granted_scopes_fallback(
            session, shop_domain, access_token
        )
        if fallback_value:
            scope_value = fallback_value
            source = fallback_source
        else:
            source = fallback_source
    record_shopify_scope_audit(shop_name, shop_domain, scope_value, source)
    return str(scope_value or "")


def get_access_token(session, store, cache):
    domain = str(store["domain"]).strip().lower()
    client_id = store["client_id"]
    client_secret = store["client_secret"]
    shop_name = store.get("name", domain)

    # Clear any prior failure record for this store in the current run.
    with TOKEN_FAILURES_LOCK:
        TOKEN_FAILURES.pop(domain, None)

    cached = cache.get(domain) or {}
    cached_token = cached.get("access_token")
    expires_at = float(cached.get("expires_at", 0) or 0)
    cached_scope = str(cached.get("scope") or "")
    cached_scopes = parse_shopify_scopes(cached_scope)
    if cached_token and expires_at > time.time() + 300 and "read_all_orders" in cached_scopes:
        audit_cached_or_new_scope(
            session, shop_name, domain, cached_token, cached_scope, "token_cache"
        )
        return cached_token

    token_url = f"https://{domain}/admin/oauth/access_token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }

    try:
        resp = safe_request(session, "POST", token_url, json=payload)
        if resp is not None and resp.status_code == 200:
            token_data = resp.json()
            access_token = token_data.get("access_token")
            if access_token:
                expires_in = int(token_data.get("expires_in", 86399) or 86399)
                granted_scope = str(token_data.get("scope") or "")
                granted_scope = audit_cached_or_new_scope(
                    session, shop_name, domain, access_token, granted_scope, "token_response"
                )
                cache[domain] = {
                    "access_token": access_token,
                    "expires_at": time.time() + expires_in,
                    "scope": granted_scope,
                }
                save_token_cache(cache)
                return access_token

            failure = {
                "http_status": 200,
                "error_type": "TokenResponseError",
                "error_details": "Token endpoint returned HTTP 200 without access_token",
            }
            print(f"[{shop_name}] Token 接口返回 200，但没有 access_token")
        elif resp is not None:
            failure = {
                "http_status": int(resp.status_code),
                "error_type": "OAuthHTTPError",
                "error_details": (resp.text or "")[:500] or f"HTTP {resp.status_code}",
            }
            print(f"[{shop_name}] Token 获取失败 HTTP {resp.status_code}: {(resp.text or '')[:180]}")
        else:
            failure = {
                "http_status": 0,
                "error_type": "OAuthNoResponse",
                "error_details": "Token endpoint returned no HTTP response",
            }
    except Exception as exc:
        failure = {
            "http_status": 0,
            "error_type": type(exc).__name__,
            "error_details": str(exc)[:500],
        }
        print(f"[{shop_name}] 请求 Token 异常: {type(exc).__name__}: {str(exc)[:180]}")

    with TOKEN_FAILURES_LOCK:
        TOKEN_FAILURES[domain] = failure
    record_shopify_scope_audit(shop_name, domain, "", "token_exchange_failed")
    return None


def calculate_successful_refunds(refunds, expected_currency=""):
    """Sum actual successful refund transactions, not item subtotals.

    Shopify refund transactions include the money actually returned, including
    possible shipping refunds. Failed/pending transactions are not counted.
    """
    total = Decimal("0")
    latest_refund_at = ""
    successful_transaction_count = 0
    expected_currency = str(expected_currency or "").strip().upper()

    for refund in refunds or []:
        refund_created_at = str(refund.get("created_at") or "")
        for txn in refund.get("transactions") or []:
            if str(txn.get("kind") or "").strip().lower() != "refund":
                continue
            if str(txn.get("status") or "").strip().lower() != "success":
                continue

            txn_currency = str(txn.get("currency") or "").strip().upper()
            if expected_currency and txn_currency and txn_currency != expected_currency:
                print(
                    "⚠️ [退款币种不一致] 跳过无法直接相加的退款交易："
                    f"退款币种={txn_currency}，案件币种={expected_currency}，交易ID={txn.get('id', '')}"
                )
                continue

            try:
                amount = Decimal(str(txn.get("amount") or "0"))
            except (InvalidOperation, TypeError, ValueError):
                print(f"⚠️ 退款交易金额无法解析，交易ID={txn.get('id', '')}，金额={txn.get('amount')!r}")
                continue

            if amount <= 0:
                continue

            total += amount
            successful_transaction_count += 1
            txn_time = str(txn.get("processed_at") or txn.get("created_at") or refund_created_at)
            if txn_time > latest_refund_at:
                latest_refund_at = txn_time

    return float(total.quantize(Decimal("0.01"))), successful_transaction_count, latest_refund_at

def get_order_details(session, shop_name, shop_domain, access_token, order_id, evidence_due_date="", raw_reason=""):
    """Read order enrichment fields once; return a loaded flag for safe sheet preservation."""
    if not order_id:
        return "N/A", 0.0, "N/A", "N/A", [], "", "无", "无", False

    url = f"https://{shop_domain}/admin/api/{API_VERSION}/orders/{order_id}.json?fields=id,name,total_price,total_price_set,customer,refunds,tags,fulfillments"
    headers = {"X-Shopify-Access-Token": access_token}
    try:
        res = safe_request(
            session, "GET", url, headers=headers,
            max_retries=3, retry_delay=0.25, timeout=(5, 10)
        )
        if res is not None and res.status_code == 200:
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
            tags_list = _split_order_tags(raw_tags)
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
                    formatted_tags, carrier_str, tracking_str, True)

        if res is not None:
            status_code = res.status_code
            response_headers = res.headers or {}
            issue = {
                "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "shop_name": shop_name,
                "shop_domain": shop_domain,
                "order_id": str(order_id),
                "stage": "GET_ORDER_DETAILS",
                "http_status": status_code,
                "retry_after": response_headers.get("Retry-After", ""),
                "api_call_limit": response_headers.get("X-Shopify-Shop-Api-Call-Limit", ""),
                "request_id": response_headers.get("X-Request-ID", ""),
                "response": (res.text or "")[:180],
                "diagnostic_hint": (
                    "若该订单创建时间超过60天且Token缺少read_all_orders，可能是历史订单访问范围限制；"
                    "若已授予read_all_orders仍返回404，请核对order_id是否属于该店铺、订单是否存在及当前应用安装。"
                    if status_code == 404 else ""
                ),
            }
        else:
            issue = {
                "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "shop_name": shop_name,
                "shop_domain": shop_domain,
                "order_id": str(order_id),
                "stage": "GET_ORDER_DETAILS",
                "http_status": None,
                "exception": "safe_request returned no response",
            }
        increment_audit_counter("order_detail_read_failure")
        with FILE_LOCK:
            with open("shopify_order_read_errors.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(issue, ensure_ascii=False) + "\n")

    except Exception as exc:
        increment_audit_counter("order_detail_read_failure")
        issue = {
            "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "shop_name": shop_name,
            "shop_domain": shop_domain,
            "order_id": str(order_id),
            "stage": "GET_ORDER_DETAILS",
            "exception": type(exc).__name__,
            "error": str(exc)[:180],
        }
        with FILE_LOCK:
            with open("shopify_order_read_errors.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(issue, ensure_ascii=False) + "\n")

    return "N/A", 0.0, "N/A", "N/A", [], "", "无", "无", False

def fetch_disputes_for_shop(store, token_cache):
    shop_name = store.get("name", store["domain"])
    shop_domain = store["domain"]
    session = get_session()
    
    access_token = get_access_token(session, store, token_cache)
    parsed_disputes = []
    error_logs = []
    
    if not access_token:
        with TOKEN_FAILURES_LOCK:
            failure = dict(TOKEN_FAILURES.get(str(shop_domain).strip().lower()) or {})
        http_status = int(failure.get("http_status", 0) or 0)
        error_type = failure.get("error_type", "OAuthTokenError")
        details = failure.get("error_details", "Token exchange failed without a recorded response")
        err_msg = f"Client Credentials Token 获取失败（HTTP {http_status or '无响应'}）：{details[:240]}"
        explanation = translate_error(error_type, http_status, details)
        failed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        error_logs.append({
            "time": failed_at,
            "shop_name": shop_name,
            "shop_domain": shop_domain,
            "api": "OAuth Token Exchange",
            "http_status": http_status,
            "error_type": error_type,
            "error_details": details[:500],
            "retry_count": 5,
            "error_explanation": explanation
        })
        sync_status = {
            "shop_name": shop_name,
            "shop_domain": shop_domain,
            "last_success_at": "",
            "last_failed_at": failed_at,
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
            
            if response is None or response.status_code != 200:
                status_code = response.status_code if response is not None else 0
                err_details = response.text[:200] if response is not None else "网络连接超时或响应为空"
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

        # 用 GraphQL 同时核验案件原因、类型和状态；GraphQL 数据作为规范来源。
        # 任一 REST 案件无法在 GraphQL 匹配时，本店本轮停止同步，避免错误状态写入。
        if raw_dispute_items:
            graphql_details = fetch_graphql_dispute_details(session, shop_domain, access_token)
            missing_ids = [str(item.get("id", "")) for item in raw_dispute_items
                           if str(item.get("id", "")) not in graphql_details]
            incomplete_ids = [
                str(item.get("id", "")) for item in raw_dispute_items
                if str(item.get("id", "")) in graphql_details
                and (
                    not graphql_details[str(item.get("id", ""))].get("reason")
                    or not graphql_details[str(item.get("id", ""))].get("type")
                    or not graphql_details[str(item.get("id", ""))].get("status")
                )
            ]
            if missing_ids or incomplete_ids:
                sample = (missing_ids + incomplete_ids)[:20]
                raise RuntimeError(
                    "GraphQL 拒付案件与 REST 列表未能完整匹配，或 GraphQL 缺少原因/类型/状态；"
                    "为避免错误写入，本店本轮不做同步。"
                    f"案件ID示例: {sample}。请确认 GraphQL 权限及 API 数据完整性。"
                )

            for dispute_item in raw_dispute_items:
                dispute_id = str(dispute_item.get("id", ""))
                canonical = graphql_details[dispute_id]

                rest_reason = normalize_reason_code(dispute_item.get("reason"))
                graphql_reason = canonical["reason"]
                network_code = canonical.get("network_reason_code", "")

                rest_type = str(dispute_item.get("type") or "").strip().upper()
                rest_status = str(dispute_item.get("status") or "").strip().upper()
                graphql_type = canonical["type"]
                graphql_status = canonical["status"]
                finalized_on = canonical.get("finalized_on", "")
                # Use the exact ShopifyPaymentsDispute node as the current case source.
                # OrderDisputeSummary.initiatedAs describes the initial type and must not
                # overwrite Dispute.type after an inquiry escalates to a chargeback.
                selected_type = graphql_type
                selected_status = graphql_status

                if (rest_reason and rest_reason != graphql_reason) or dispute_id in REASON_AUDIT_TARGET_IDS:
                    log_reason_mismatch(
                        shop_name, shop_domain, dispute_item,
                        rest_reason, graphql_reason, network_code
                    )

                log_dispute_status_audit(
                    shop_name, shop_domain, dispute_item,
                    rest_type, rest_status, graphql_type, graphql_status, finalized_on
                )

                dispute_item["_rest_reason"] = rest_reason
                dispute_item["_rest_type"] = rest_type
                dispute_item["_rest_status"] = rest_status
                dispute_item["_graphql_type"] = graphql_type
                dispute_item["_graphql_status"] = graphql_status
                dispute_item["_finalized_on"] = finalized_on
                dispute_item["_order_alerts"] = canonical.get("order_alerts", [])
                dispute_item["reason"] = graphql_reason
                dispute_item["type"] = selected_type
                dispute_item["status"] = selected_status
                dispute_item["network_reason_code"] = network_code

        # 每个 Shopify 订单本轮只读取一次详情；同订单存在多个案件时共享同一份快照。
        # 这能避免同一订单重复 GET，减少 Shopify REST 请求量和 429 风险。
        order_details_by_id = {}
        if raw_dispute_items:
            unique_order_ids = sorted({
                str(item.get("order_id"))
                for item in raw_dispute_items
                if item.get("order_id")
            })

            if unique_order_ids:
                order_fetch_workers = min(MAX_ORDER_WORKERS, len(unique_order_ids))
                with ThreadPoolExecutor(max_workers=order_fetch_workers) as details_executor:
                    detail_futures = {
                        details_executor.submit(
                            get_order_details,
                            session, shop_name, shop_domain, access_token, order_id
                        ): order_id
                        for order_id in unique_order_ids
                    }
                    for future in as_completed(detail_futures):
                        order_id = detail_futures[future]
                        try:
                            order_details_by_id[order_id] = future.result()
                        except Exception as detail_exc:
                            print(
                                f"[{shop_name}] 订单详情任务异常（订单 {order_id}）："
                                f"{type(detail_exc).__name__}: {str(detail_exc)[:120]}"
                            )
                            order_details_by_id[order_id] = (
                                "N/A", 0.0, "N/A", "N/A", [], "", "无", "无", False
                            )

            def process_single_dispute(d):
                dispute_id = str(d["id"])
                raw_type = str(d.get("type") or d.get("_graphql_type") or "CHARGEBACK").upper()
                raw_status = str(d.get("status") or d.get("_graphql_status") or "").upper()
                raw_reason = normalize_reason_code(d.get("reason", ""))

                type_cn = TYPE_MAP.get(raw_type, raw_type)
                status_cn = translate_dispute_status(raw_type, raw_status)
                admin_alerts = d.get("_order_alerts") or []
                admin_alert_status, admin_alert_source = select_shopify_admin_dispute_alert(
                    raw_type, raw_status, admin_alerts
                )
                if admin_alert_status:
                    status_cn = admin_alert_status
                    increment_audit_counter("admin_alert_status_override")
                    audit_entry = {
                        "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "shop_name": shop_name,
                        "shop_domain": shop_domain,
                        "order_id": str(d.get("order_id", "")),
                        "dispute_id": dispute_id,
                        "graphql_type": raw_type,
                        "graphql_status": raw_status,
                        "selected_status": status_cn,
                        "selected_status_source": admin_alert_source,
                        "order_alerts": admin_alerts,
                    }
                    with FILE_LOCK:
                        with open("dispute_admin_alert_status_audit.jsonl", "a", encoding="utf-8") as f:
                            f.write(json.dumps(audit_entry, ensure_ascii=False) + "\n")
                reason_cn = REASON_MAP.get(raw_reason, raw_reason if raw_reason else "未知原因")
                
                evidence_due_date = d.get("evidence_due_by", "") or "无"
                unique_key = f"{shop_domain}|{dispute_id}"
                order_id = d.get("order_id")
                
                if order_id:
                    order_detail_snapshot = order_details_by_id.get(str(order_id))
                else:
                    order_detail_snapshot = None

                if order_detail_snapshot is None:
                    order_detail_snapshot = (
                        "N/A", 0.0, "N/A", "N/A", [], "", "无", "无", False
                    )

                (order_name, order_total, cust_name, cust_email,
                 refunds, order_tags, carrier, tracking_number, order_tags_loaded) = order_detail_snapshot
                
                refund_count = len(refunds)  # Shopify Refund 记录数，保持原表格口径
                total_refunded, successful_refund_txn_count, last_refund_at = calculate_successful_refunds(
                    refunds, expected_currency=d.get("currency", "")
                )
                # 只有实际成功的退款交易金额大于 0，才标记为已退款。
                is_refunded = "是" if total_refunded > 0 else "否"
                is_partially_refunded = (
                    "是" if (0 < total_refunded and total_refunded + 0.01 < order_total) else "否"
                )

                # Inquiry + LOST 不等于正式拒付败诉；结合订单实际退款金额显示问询关闭状态。
                if raw_type == "INQUIRY" and raw_status == "LOST":
                    if order_total > 0 and total_refunded + 0.01 >= order_total:
                        status_cn = "查询已关闭（订单已全额退款）"
                    elif total_refunded > 0:
                        status_cn = "查询已关闭（订单已有退款，状态需核实）"
                    else:
                        status_cn = "查询已关闭（状态需核实）"

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
                    "order_details_loaded": bool(order_tags_loaded),
                    "carrier": carrier,
                    "tracking_number": tracking_number,
                    "_order_id": order_id,
                    "_order_tags_loaded": order_tags_loaded,
                    "_raw_reason": raw_reason,
                    "_network_reason_code": str(d.get("network_reason_code", "") or ""),
                    "_rest_reason": str(d.get("_rest_reason", "") or ""),
                    "_rest_type": str(d.get("_rest_type", "") or ""),
                    "_rest_status": str(d.get("_rest_status", "") or ""),
                    "_graphql_type": str(d.get("_graphql_type", "") or ""),
                    "_graphql_status": str(d.get("_graphql_status", "") or ""),
                    "_finalized_on": str(d.get("_finalized_on", "") or ""),
                    "_order_alerts": d.get("_order_alerts", []) or [],
                    "_admin_alert_status_source": admin_alert_source,
                    "_successful_refund_transactions": successful_refund_txn_count,
                    "_evidence_due_date": evidence_due_date,
                    "_initiated_at": d.get("initiated_at", "")
                }

            # 订单详情已按唯一 order_id 并发读取；这里仅做本地字段转换，不再发 API 请求。
            for item in raw_dispute_items:
                try:
                    parsed_disputes.append(process_single_dispute(item))
                except Exception as sub_e:
                    print(f"[{shop_name}] 处理单条拒付解析异常：{type(sub_e).__name__}: {str(sub_e)[:160]}")

            # 先独立解析每条案件，再按订单聚合标签；避免同订单多个案件并发覆盖。
            sync_order_tags_for_disputes(session, shop_name, shop_domain, access_token, parsed_disputes)
            unresolved_details = sum(1 for item in parsed_disputes if not item.get("order_details_loaded", False))
            if unresolved_details:
                print(
                    f"[{shop_name}] 本轮有 {unresolved_details} 条案件未能取得订单详情快照；"
                    "旧表格中的订单补充字段将保留，具体订单和 HTTP 状态见诊断文件。"
                )

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
        return True

    all_batches_success = True
    headers = {"Content-Type": "application/json"}
    total = len(item_list)

    for i in range(0, total, batch_size):
        chunk = item_list[i:i + batch_size]
        payload = {"action": action, item_key: chunk}
        success = False
        
        for attempt in range(1, max_retries + 1):
            # 🚀 单批次变大，适当放宽超时时间到 90 秒
            # 外层循环负责重试；单次请求不再叠加 safe_request 的内部重试，避免重试次数相乘。
            try:
                res = safe_request(
                    session, "POST", GAS_WEBHOOK_URL, json=payload, headers=headers,
                    max_retries=1, timeout=(10, 90)
                )
            except Exception as exc:
                res = None
                print(f"[{action}] GAS 请求异常（第 {attempt}/{max_retries} 次）：{type(exc).__name__}: {exc}")

            if res is not None and res.status_code == 200:
                try:
                    res_json = res.json()
                    if res_json.get("status") == "success":
                        print(f"[{action}] 批次 ({i+1}-{min(i+batch_size, total)}/{total}) 推送成功")
                        success = True
                        break
                    elif res_json.get("status") == "error" and "Lock timeout" in res_json.get("message", ""):
                        wait = 3 * (2 ** (attempt - 1))
                        if attempt < max_retries:
                            print(f"[{action}] GAS 服务繁忙 (Lock timeout)，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                            time.sleep(wait)
                        continue
                    else:
                        wait = 3 * attempt
                        if attempt < max_retries:
                            print(f"[{action}] GAS 返回了不符合预期的响应: {res.text[:100]}，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                            time.sleep(wait)
                        continue
                except Exception:
                    wait = 3 * attempt
                    if attempt < max_retries:
                        print(f"[{action}] GAS 返回了非 JSON 响应，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                        time.sleep(wait)
                    continue
            else:
                wait = 3 * attempt
                if attempt < max_retries:
                    print(f"[{action}] 推送异常 (Status: {res.status_code if res is not None else '无HTTP响应'})，等待 {wait} 秒后重试 (第 {attempt}/{max_retries} 次)...")
                    time.sleep(wait)
                
        if not success:
            all_batches_success = False
            print(f"❌ [{action}] 批次 ({i+1}-{min(i+batch_size, total)}/{total}) 经过 {max_retries} 次重试后仍然失败，请检查 GAS 后端限制！")

        time.sleep(0.1)  # 🚀 将批次间歇缩短至 0.1 秒，大幅提速

    return all_batches_success

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

    with SCOPE_AUDIT_LOCK:
        scope_results = list(SCOPE_AUDIT_RESULTS.values())
    missing_all_orders = [r for r in scope_results if r.get("scope_known") and not r.get("has_read_all_orders")]
    missing_read_orders = [r for r in scope_results if r.get("scope_known") and not r.get("has_read_orders_or_write_orders")]
    unknown_scope = [r for r in scope_results if not r.get("scope_known")]
    print(
        f"[Shopify 权限核验] 已检查 {len(scope_results)} 家店铺；"
        f"缺少 read_all_orders={len(missing_all_orders)} 家；"
        f"缺少 read_orders/write_orders={len(missing_read_orders)} 家；"
        f"权限信息未知={len(unknown_scope)} 家。"
    )
    if missing_all_orders:
        print("[历史订单权限缺失] " + ", ".join(r["shop_name"] for r in missing_all_orders))
    if missing_read_orders:
        print("[订单基础读取权限缺失] " + ", ".join(r["shop_name"] for r in missing_read_orders))
    if unknown_scope:
        print("[权限核验未取得结果] " + ", ".join(r["shop_name"] for r in unknown_scope))

    if all_error_logs:
        print("[店铺/API异常明细] 以下异常已同时写入 LOG_ERROR：")
        for err in all_error_logs:
            print(
                f"[API异常] 店铺={err.get('shop_name', '')} "
                f"接口={err.get('api', '')} HTTP={err.get('http_status', '')} "
                f"类型={err.get('error_type', '')} "
                f"详情={str(err.get('error_details', ''))[:160]}"
            )

    with COUNTER_LOCK:
        audit_snapshot = dict(AUDIT_COUNTERS)
    print(
        "[异常汇总] "
        f"订单详情读取失败={audit_snapshot.get('order_detail_read_failure', 0)}；"
        f"标签写回失败={audit_snapshot.get('tag_write_failure', 0)}；"
        f"因缺少订单快照而跳过标签写回={audit_snapshot.get('tag_write_skipped_no_snapshot', 0)}；"
        f"REST/GraphQL原因差异={audit_snapshot.get('reason_source_mismatch', 0)}；"
        f"REST/GraphQL案件状态差异={audit_snapshot.get('status_source_mismatch', 0)}；"
        f"查询+LOST待核实={audit_snapshot.get('inquiry_lost_review', 0)}。"
    )

    session = get_session()
    
    if all_disputes:
        print(f"[SYNC_DISPUTES] 准备向表格推送 {len(all_disputes)} 条案件记录。")
        sync_success = post_to_gas(session, "SYNC_DISPUTES", "disputes", all_disputes, batch_size=500)
        if not sync_success:
            raise RuntimeError("拒付案件数据未能完整推送到 Google Sheets；本次工作流应标记为失败并检查 GAS 日志。")
    else:
        print("[SYNC_DISPUTES] 本轮没有抓取到可推送的案件记录。")
        
    if all_sync_statuses:
        post_to_gas(session, "UPDATE_SYNC_STATUS", "sync_status", all_sync_statuses, batch_size=500)
        
    if all_error_logs:
        post_to_gas(session, "LOG_ERROR", "error_logs", all_error_logs, batch_size=500)

if __name__ == "__main__":
    main()