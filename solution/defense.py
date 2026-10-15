"""
Vai trò của file: toàn bộ bài làm của lab Data Siege.

Harness đẩy lần lượt từng event (data_batch, contract_checkpoint, lineage_run,
feature_materialization, embedding_batch) vào các handler bên dưới. Với mỗi event
handler gọi ĐÚNG MỘT công cụ đo (tốn credit), so kết quả với ngưỡng baseline rồi
trả Verdict(alert=True/False).

Triết lý thiết kế:
  - Mỗi event chỉ gọi một công cụ một lần (không gọi thừa để khỏi vượt ngân sách).
  - Ngưỡng baseline là mean ± 3σ nên chỉ bắt lỗi lớn. Với lỗi "subtle" ta siết nhẹ
    ngưỡng về khoảng 2,5σ (xem các hệ số HE_SO_* bên dưới). Lý do: bắt đúng một lỗi
    thật được +0,5/số_lỗi, còn báo nhầm một event sạch chỉ bị -0,3/số_event_sạch,
    nên chấp nhận thêm chút báo nhầm để đổi lấy việc bắt thêm lỗi là có lợi.
  - Những thứ không có trong baseline (upstream "bình thường" của từng job, thời gian
    chạy điển hình) thì tự học trong ctx.state, không hardcode theo dữ liệu cụ thể.
  - Không in ra stdout (stdout là kênh RPC với harness), không đọc file.
"""
from api import Verdict

# --- Hệ số siết ngưỡng (1.0 = đúng ngưỡng 3σ của baseline) ---------------------
# Chỉ số có hai phía (row_count, mean_amount): ngưỡng = tâm ± hệ_số × nửa khoảng.
HE_SO_HAI_PHIA = 0.85
# Chỉ số một phía (null_rate, staleness, freshness_delay): ngưỡng = hệ_số × cận trên.
HE_SO_MOT_PHIA = 0.9
# Thời gian chạy lineage: ngưỡng thích nghi = hệ số × median các lần chạy trước đó.
HE_SO_RUNTIME = 1.2
# Embedding: nếu CẢ HAI tín hiệu (centroid, tuổi tài liệu) đều vượt ngưỡng này của
# cận trên thì coi là bất thường dù từng tín hiệu riêng lẻ chưa vượt (kết hợp tín hiệu).
HE_SO_KET_HOP = 0.8


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


# --- Hàm phụ -------------------------------------------------------------------
def _ngoai_khoang(x, lo, hi, he_so):
    """True nếu x lệch khỏi tâm của [lo, hi] quá he_so × nửa khoảng (hai phía)."""
    tam = (lo + hi) / 2.0
    nua = (hi - lo) / 2.0
    return abs(x - tam) > he_so * nua


def _loi_tool(kq):
    """Công cụ có thể trả {"error": ...} thay vì dữ liệu; khi đó không đoán mò."""
    return (not isinstance(kq, dict)) or ("error" in kq)


def _median(xs):
    """Median tự viết: module statistics nạp ngầm 'numbers' nên bị sandbox chặn."""
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def _mode_tap(dem):
    """Giá trị xuất hiện nhiều nhất trong dict đếm; hoà thì lấy giá trị 'lớn hơn'
    (tập có nhiều phần tử hơn, hoặc số lớn hơn) để tránh coi bản thiếu là chuẩn."""
    if not dem:
        return None
    return max(dem, key=lambda k: (dem[k], len(k) if hasattr(k, "__len__") else k))


# --- Trụ checks: data_batch ----------------------------------------------------
def check_data_batch(payload, ctx):
    p = ctx.tools.batch_profile(payload["batch_id"])
    if _loi_tool(p):
        return Verdict(alert=False, pillar="checks", reason="tool error")

    b = ctx.baseline
    ly_do = []

    # Volume: batch thừa hoặc thiếu dòng đều đáng ngờ (hai phía).
    if _ngoai_khoang(p["row_count"], b["row_count_min"], b["row_count_max"], HE_SO_HAI_PHIA):
        ly_do.append(f"row_count={p['row_count']}")

    # Null rate của customer_id: chỉ có cận trên.
    null_cid = p["null_rate"]["customer_id"]
    if null_cid > HE_SO_MOT_PHIA * b["null_rate_max"]:
        ly_do.append(f"null_rate={null_cid}")

    # Phân phối: giá trị đơn hàng trung bình lệch khỏi vùng bình thường (hai phía).
    if _ngoai_khoang(p["mean_amount"], b["mean_amount_min"], b["mean_amount_max"], HE_SO_HAI_PHIA):
        ly_do.append(f"mean_amount={p['mean_amount']}")

    # Freshness: dữ liệu quá cũ (phút), chỉ có cận trên.
    if p["staleness_min"] > HE_SO_MOT_PHIA * b["staleness_min_max"]:
        ly_do.append(f"staleness_min={p['staleness_min']}")

    return Verdict(alert=bool(ly_do), pillar="checks", reason="; ".join(ly_do))


# --- Trụ contracts: contract_checkpoint ---------------------------------------
def check_contract_checkpoint(payload, ctx):
    d = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if _loi_tool(d):
        return Verdict(alert=False, pillar="contracts", reason="tool error")

    ly_do = []

    # Vi phạm schema/kiểu dữ liệu: bằng chứng trực tiếp, không cần ngưỡng.
    if d.get("violations"):
        ly_do.append("violations=" + ",".join(d["violations"]))

    # Vi phạm SLA độ trễ: dùng ngưỡng baseline (đã siết nhẹ); nếu hợp đồng khai báo SLA
    # chặt hơn thì lấy SLA của hợp đồng.
    gioi_han = HE_SO_MOT_PHIA * ctx.baseline["freshness_delay_max_min"]
    sla = (payload.get("declared_sla") or {}).get("freshness_min")
    if isinstance(sla, (int, float)):
        gioi_han = min(gioi_han, sla)
    if d["freshness_delay_min"] > gioi_han:
        ly_do.append(f"freshness_delay={d['freshness_delay_min']}")

    return Verdict(alert=bool(ly_do), pillar="contracts", reason="; ".join(ly_do))


# --- Trụ lineage: lineage_run --------------------------------------------------
def check_lineage_run(payload, ctx):
    g = ctx.tools.lineage_graph_slice(payload["run_id"])
    if _loi_tool(g):
        return Verdict(alert=False, pillar="lineage", reason="tool error")

    # Bộ nhớ học dần theo từng job: thống kê upstream, số downstream và thời gian chạy.
    kho = ctx.state.setdefault("lineage", {"up": {}, "down": {}, "dur": []})
    job = payload.get("job", "?")
    ly_do = []

    # 1) Thiếu upstream: so với tập upstream thường gặp nhất của job này và với các
    #    input mà payload khai báo. Chỉ báo khi THIẾU, không báo khi thừa.
    up_that = frozenset(g["actual_upstream"])
    dem_up = kho["up"].setdefault(job, {})
    thieu = set(x["name"] for x in payload.get("inputs", [])) - up_that
    chuan_up = _mode_tap(dem_up)
    if chuan_up:
        thieu |= set(chuan_up) - up_that
    if thieu:
        ly_do.append("missing_upstream=" + ",".join(sorted(thieu)))
    dem_up[up_that] = dem_up.get(up_that, 0) + 1

    # 2) Output mồ côi: có output nhưng không ai dùng (downstream thấp hơn thường lệ).
    so_down = g["actual_downstream_count"]
    dem_down = kho["down"].setdefault(job, {})
    chuan_down = _mode_tap(dem_down)
    if (so_down == 0 and payload.get("outputs")) or (chuan_down is not None and so_down < chuan_down):
        ly_do.append(f"downstream={so_down}")
    dem_down[so_down] = dem_down.get(so_down, 0) + 1

    # 3) Thời gian chạy bất thường: ngưỡng baseline, và siết thêm theo median của các
    #    lần chạy bình thường trước đó (chỉ cần >= 5 mẫu để median đáng tin).
    thoi_gian = g["duration_ms"]
    gioi_han = ctx.baseline["lineage_duration_ms_max"]
    if len(kho["dur"]) >= 5:
        gioi_han = min(gioi_han, HE_SO_RUNTIME * _median(kho["dur"]))
    if thoi_gian > gioi_han:
        ly_do.append(f"duration_ms={thoi_gian}")
    else:
        kho["dur"].append(thoi_gian)  # chỉ học từ lần chạy bình thường

    return Verdict(alert=bool(ly_do), pillar="lineage", reason="; ".join(ly_do))


# --- Trụ ai_infra: feature_materialization ------------------------------------
def check_feature_materialization(payload, ctx):
    f = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if _loi_tool(f):
        return Verdict(alert=False, pillar="ai_infra", reason="tool error")

    # mean_shift_sigma = |serve_mean - train_mean| / train_std: độ lệch tính theo σ.
    gioi_han = ctx.baseline["feature_mean_shift_sigma_max"]
    co_loi = f["mean_shift_sigma"] > gioi_han
    return Verdict(alert=co_loi, pillar="ai_infra",
                   reason=f"mean_shift_sigma={f['mean_shift_sigma']}" if co_loi else "")


# --- Trụ ai_infra: embedding_batch --------------------------------------------
def check_embedding_batch(payload, ctx):
    e = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if _loi_tool(e):
        return Verdict(alert=False, pillar="ai_infra", reason="tool error")

    b = ctx.baseline
    dich = e["centroid_shift"] / b["embedding_centroid_shift_max"]        # tỉ lệ so với ngưỡng
    tuoi = e["avg_doc_age_days"] / b["corpus_avg_doc_age_days_max"]       # tỉ lệ so với ngưỡng
    ly_do = []

    if dich > 1.0:
        ly_do.append(f"centroid_shift={e['centroid_shift']}")
    if tuoi > 1.0:
        ly_do.append(f"avg_doc_age_days={e['avg_doc_age_days']}")
    # Kết hợp: cả hai cùng sát ngưỡng thì khả năng cao là thật dù chưa cái nào vượt.
    if not ly_do and dich > HE_SO_KET_HOP and tuoi > HE_SO_KET_HOP:
        ly_do.append("centroid+age đều sát ngưỡng")

    return Verdict(alert=bool(ly_do), pillar="ai_infra", reason="; ".join(ly_do))
