# ================= YANDEX FLEET API (ОБНОВЛЁННЫЙ ПОДСЧЁТ ПРОБЕГА И СТАТИСТИКИ) =================
def _safe_float(val: Any) -> float:
    if val is None or isinstance(val, bool):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, dict):
        for k in ("km", "kilometers", "meters", "amount", "value", "total", "price", "distance"):
            if k in val and val[k] is not None:
                v = _safe_float(val[k])
                if v > 0:
                    return v / 1000.0 if k == "meters" else v
        return 0.0
    cleaned = re.sub(r"[^\d.-]", "", str(val).replace(" ", "").replace(",", "."))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _normalize_dist_value_to_km(raw_val: Any, unit_hint: str = "") -> float:
    v = _safe_float(raw_val)
    if v <= 0:
        return 0.0
    uh = (unit_hint or "").lower()
    if "meter" in uh or "метр" in uh or uh in ("m", "meters"):
        return v / 1000.0
    if "km" in uh or "километр" in uh or uh == "kilometers":
        return v
    if v >= 200:
        return v / 1000.0
    return v


def _extract_order_distance_km(order: dict[str, Any]) -> float:
    if not isinstance(order, dict):
        return 0.0

    direct_km_keys = (
        "distance_km",
        "mileage_km",
        "route_length_km",
        "paid_distance_km",
        "total_distance_km",
        "order_distance_km",
    )
    for key in direct_km_keys:
        if order.get(key) is not None:
            v = _safe_float(order[key])
            if v > 0:
                return v

    dist_keys = (
        "distance",
        "mileage",
        "route_length",
        "paid_distance",
        "total_distance",
        "pickup_distance",
        "delivery_distance",
        "order_distance",
    )
    total_km = 0.0
    for key in dist_keys:
        raw_v = order.get(key)
        if raw_v is None:
            continue
        if isinstance(raw_v, dict):
            if raw_v.get("km") is not None:
                v = _safe_float(raw_v["km"])
                if v > 0:
                    total_km += v
                    continue
            if raw_v.get("kilometers") is not None:
                v = _safe_float(raw_v["kilometers"])
                if v > 0:
                    total_km += v
                    continue
            if raw_v.get("meters") is not None:
                v = _safe_float(raw_v["meters"]) / 1000.0
                if v > 0:
                    total_km += v
                    continue
            unit_str = str(raw_v.get("unit") or raw_v.get("units") or "")
            for sub_k in ("value", "amount", "distance"):
                if raw_v.get(sub_k) is not None:
                    sv = _normalize_dist_value_to_km(raw_v[sub_k], unit_str)
                    if sv > 0:
                        total_km += sv
                        break
        else:
            v = _normalize_dist_value_to_km(raw_v, key)
            if v > 0:
                total_km += v

    if total_km > 0:
        return total_km

    for nested_key in ("route", "metrics", "stats", "delivery"):
        nested_obj = order.get(nested_key)
        if isinstance(nested_obj, dict):
            for rk in ("distance_km", "km", "kilometers", "meters", "distance", "length"):
                if nested_obj.get(rk) is not None:
                    rv = _normalize_dist_value_to_km(nested_obj[rk], rk)
                    if rv > 0:
                        return rv

    return 0.0


async def _get_driver_orders_stats(
    session: aiohttp.ClientSession,
    park: dict[str, str],
    driver_id: str,
    goal_start_dt: datetime,
    force_refresh: bool = False,
) -> dict[str, Any]:
    cache_key = f"{park['park_id']}:{driver_id}"
    async with _STATS_LOCKS[cache_key]:
        now_mono = time.monotonic()
        cached = DRIVER_STATS_CACHE.get(cache_key)
        if not force_refresh and cached and (now_mono - cached["ts"]) < DRIVER_STATS_TTL_SEC:
            return cached["data"]

        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)
        goal_end = goal_start_dt + timedelta(days=GOAL_DAYS)
        window_from = min(week_ago, goal_start_dt)
        window_to = now

        payload = {
            "query": {
                "park": {
                    "id": park["park_id"],
                    "driver_profile": {"id": driver_id},
                    "order": {
                        "booked_at": {
                            "from": window_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "to": window_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        }
                    },
                }
            },
            "limit": 500,
        }

        try:
            data = await _post_fleet_with_retry(session, park, "/v1/parks/orders/list", payload)
        except Exception as exc:
            logger.warning("orders/list ошибка для %s (парк %s): %s", driver_id, park["park_id"], exc)
            if cached and cached.get("data", {}).get("ok"):
                return cached["data"]
            return {
                "ok": False,
                "error": str(exc),
                "completed_7d": 0,
                "completed_goal": 0,
                "within_goal_window": now <= goal_end,
                "distance_km_7d": 0.0,
                "distance_estimated": False,
            }

        orders = data.get("orders") or []
        completed_7d = 0
        completed_goal = 0
        distance_km_7d = 0.0
        sample_keys_logged = False

        for order in orders:
            if not isinstance(order, dict):
                continue
            if not sample_keys_logged:
                sample_keys_logged = True
                logger.info(
                    "orders/list sample keys для driver_id=%s: %s",
                    driver_id,
                    list(order.keys()),
                )

            status = str(order.get("status") or "")
            dist_km = _extract_order_distance_km(order)
            odt = _parse_order_dt(order)

            if _is_completed_order_status(status):
                if odt is None or odt >= week_ago:
                    completed_7d += 1
                    distance_km_7d += dist_km
                if odt is None or (goal_start_dt <= odt <= goal_end):
                    completed_goal += 1

        distance_estimated = False
        if completed_7d > 0 and distance_km_7d == 0.0:
            distance_km_7d = round(completed_7d * 4.5, 1)
            distance_estimated = True

        result = {
            "ok": True,
            "error": "",
            "completed_7d": completed_7d,
            "completed_goal": completed_goal,
            "within_goal_window": now <= goal_end,
            "distance_km_7d": round(distance_km_7d, 1),
            "distance_estimated": distance_estimated,
        }
        logger.info(
            "stats для park=%s driver_id=%s: orders_total=%s completed_7d=%s distance_km_7d=%s est=%s",
            park["park_id"],
            driver_id,
            len(orders),
            completed_7d,
            result["distance_km_7d"],
            distance_estimated,
        )
        DRIVER_STATS_CACHE[cache_key] = {"ts": now_mono, "data": result}
        return result


async def show_courier_statistics(chat_id: int | str, driver: dict[str, Any]) -> None:
    park = get_park_by_id(driver["park_id"])
    goal_start = get_goal_start_dt(chat_id)
    try:
        async with aiohttp.ClientSession() as session:
            stats = await _get_driver_orders_stats(
                session, park, driver["id"], goal_start_dt=goal_start, force_refresh=True
            )
    except Exception as exc:
        logger.warning("show_courier_statistics ошибка для chat_id=%s: %s", chat_id, exc)
        stats = {
            "completed_7d": 0,
            "completed_goal": 0,
            "distance_km_7d": 0.0,
            "distance_estimated": False,
        }

    done_goal = int(stats.get("completed_goal") or 0)
    done_7d = int(stats.get("completed_7d") or 0)
    if done_goal >= GOAL_HIDE_AFTER_ORDERS or done_7d >= 10:
        save_user_session(chat_id, bonus_completed=True)

    show_bonus = await should_show_bonus_button(chat_id, driver)
    km_val = float(stats.get("distance_km_7d") or 0.0)
    km_suffix = " км (примерно)" if stats.get("distance_estimated") else " км"
    footer = (
        "Отличный темп! Если захочешь посмотреть прогресс по бонусу — жми «🎁 Бонус»."
        if show_bonus
        else "Отличный темп! Хороших смен и щедрых заказов."
    )
    text = (
        "Вот твоя свежая сводка за последние 7 дней ✨\n\n"
        f"🏢 Парк: {driver['park_name']}\n"
        f"💳 Баланс: {driver['balance']:.2f} ₽\n"
        f"📦 Заказов выполнено: {done_7d}\n"
        f"🛣 Прошёл / проехал: {km_val:.1f}{km_suffix}\n\n"
        f"{footer}"
    )
    await tg_send_text(chat_id, text, reply_markup=kb_courier_cabinet(show_bonus=show_bonus))


# ================= AMOCRM WEBHOOK И ПРИМЕЧАНИЯ (ОБНОВЛЁННЫЕ) =================
async def _fetch_latest_amocrm_note_text(
    lead_id: str | None = None,
    contact_id: str | None = None,
) -> str:
    if not AMOCRM_BASE_URL or not AMOCRM_ACCESS_TOKEN:
        logger.warning("amoCRM: не задан AMOCRM_BASE_URL или AMOCRM_ACCESS_TOKEN")
        return ""
    base_url = AMOCRM_BASE_URL.rstrip("/")
    targets: list[str] = []

    if lead_id and str(lead_id).isdigit():
        targets.append(f"{base_url}/api/v4/leads/{lead_id}/notes")
    if contact_id and str(contact_id).isdigit():
        targets.append(f"{base_url}/api/v4/contacts/{contact_id}/notes")

    try:
        async with aiohttp.ClientSession() as session:
            if not contact_id and lead_id and str(lead_id).isdigit():
                async with session.get(
                    f"{base_url}/api/v4/leads/{lead_id}",
                    params={"with": "contacts"},
                    headers=_amocrm_headers(),
                    timeout=15,
                ) as lresp:
                    if lresp.status == 200:
                        ldata = await lresp.json()
                        for c in (ldata.get("_embedded") or {}).get("contacts") or []:
                            if isinstance(c, dict) and c.get("id"):
                                c_url = f"{base_url}/api/v4/contacts/{c['id']}/notes"
                                if c_url not in targets:
                                    targets.append(c_url)

            for url in targets:
                async with session.get(
                    url,
                    params={"limit": 15, "order[created_at]": "desc"},
                    headers=_amocrm_headers(),
                    timeout=15,
                ) as resp:
                    logger.info("amoCRM notes GET %s -> status %s", url, resp.status)
                    if resp.status != 200:
                        err_txt = await resp.text()
                        logger.warning("amoCRM notes GET error body: %s", err_txt[:200])
                        continue
                    data = await resp.json()
                    notes = data.get("_embedded", {}).get("notes", []) or []
                    for n in notes:
                        if not isinstance(n, dict):
                            continue
                        params_obj = n.get("params") if isinstance(n.get("params"), dict) else {}
                        txt = str(params_obj.get("text") or n.get("text") or "").strip()
                        if not txt:
                            continue
                        if txt.startswith("Парк: ЛунапаркЕда") or txt.startswith("Обращение по кнопке"):
                            continue
                        low = txt.lower()
                        if low.startswith("tg:") or low.startswith("ответ:"):
                            logger.info("Найдено примечание с префиксом в amoCRM: %s", txt[:120])
                            return txt
                    for n in notes:
                        if not isinstance(n, dict):
                            continue
                        params_obj = n.get("params") if isinstance(n.get("params"), dict) else {}
                        txt = str(params_obj.get("text") or n.get("text") or "").strip()
                        if txt and not txt.startswith("Парк: ЛунапаркЕда") and not txt.startswith("Обращение по кнопке"):
                            logger.info("Последнее примечание без префикса в amoCRM: %s", txt[:120])
                            break
    except Exception as exc:
        logger.warning("Не удалось дочитать примечание из amoCRM: %s", exc)
    return ""


def _extract_reply_from_note(raw_note: str) -> str:
    txt = (raw_note or "").strip()
    if not txt:
        return ""
    if txt.startswith("Парк: ЛунапаркЕда") or txt.startswith("Обращение по кнопке"):
        return ""
    low = txt.lower()
    if low.startswith("tg:"):
        return txt[3:].strip()
    if low.startswith("ответ:"):
        return txt[6:].strip()
    return ""


async def handle_amocrm_webhook(request: web.Request) -> web.Response:
    if AMOCRM_WEBHOOK_SECRET:
        secret = request.query.get("secret") or request.headers.get("X-AmoCRM-Secret", "")
        if not hmac.compare_digest(secret, AMOCRM_WEBHOOK_SECRET):
            return web.json_response({"ok": False}, status=403)

    lead_id: str | None = None
    contact_id: str | None = None
    note_text = ""

    try:
        if request.content_type == "application/json":
            payload = await request.json()
        else:
            form_data = await request.post()
            payload = dict(form_data)
    except Exception:
        return web.json_response({"ok": False}, status=400)

    if isinstance(payload, dict):
        lead_id = str(payload.get("lead_id") or "") or None
        contact_id = str(payload.get("contact_id") or "") or None
        note_text = str(payload.get("text") or payload.get("note_text") or "").strip()

        for k, v in payload.items():
            ks = str(k)
            if not lead_id and ("leads[" in ks) and ks.endswith("[id]") and str(v).isdigit():
                lead_id = str(v)
            if not contact_id and ("contacts[" in ks) and ks.endswith("[id]") and str(v).isdigit():
                contact_id = str(v)
            if not note_text and ("note[" in ks or "notes[" in ks) and ks.endswith("[text]") and v:
                note_text = str(v).strip()

        for ent_key, id_attr in (("leads", "lead_id"), ("contacts", "contact_id")):
            ent_block = payload.get(ent_key)
            if isinstance(ent_block, dict):
                for action_name in ("add", "update", "status", "note"):
                    act_list = ent_block.get(action_name)
                    if isinstance(act_list, dict):
                        act_list = list(act_list.values())
                    if isinstance(act_list, list) and act_list:
                        first = act_list[0]
                        if isinstance(first, dict):
                            eid = first.get("id") or first.get("element_id")
                            if eid and id_attr == "lead_id" and not lead_id:
                                lead_id = str(eid)
                            elif eid and id_attr == "contact_id" and not contact_id:
                                contact_id = str(eid)
                            notes = first.get("notes")
                            if isinstance(notes, list) and notes and isinstance(notes[0], dict) and not note_text:
                                n_params = notes[0].get("params") if isinstance(notes[0].get("params"), dict) else {}
                                note_text = str(n_params.get("text") or notes[0].get("text") or "").strip()

    logger.info("amoCRM webhook parsed: lead_id=%s, contact_id=%s, note_text=%s", lead_id, contact_id, note_text[:80])

    target_tg_id = find_tg_chat_by_crm_entity(lead_id=lead_id, contact_id=contact_id)
    if not target_tg_id:
        phones = await _fetch_contact_phones_from_amocrm(lead_id=lead_id, contact_id=contact_id)
        target_tg_id = _find_tg_by_phone_in_users_db(phones)
        if target_tg_id and (lead_id or contact_id):
            link_crm_entity_to_tg(
                int(lead_id) if lead_id and lead_id.isdigit() else None,
                int(contact_id) if contact_id and contact_id.isdigit() else None,
                target_tg_id,
            )

    if not target_tg_id:
        logger.info("amoCRM webhook ignored: no_tg_binding (lead_id=%s, contact_id=%s)", lead_id, contact_id)
        return web.json_response({"ok": True, "ignored": "no_tg_binding", "lead_id": lead_id, "contact_id": contact_id})

    clean_reply = _extract_reply_from_note(note_text)
    if not clean_reply:
        fetched_note = await _fetch_latest_amocrm_note_text(lead_id=lead_id, contact_id=contact_id)
        clean_reply = _extract_reply_from_note(fetched_note)

    if not clean_reply:
        logger.info("amoCRM webhook ignored: no_tg_or_reply_prefix (lead_id=%s, contact_id=%s)", lead_id, contact_id)
        return web.json_response({"ok": True, "ignored": "no_tg_or_reply_prefix"})

    sent_mid = await tg_send_text(target_tg_id, f"💬 Сообщение от менеджера парка:\n\n{clean_reply}")
    logger.info("amoCRM webhook sent to tg=%s, msg_id=%s", target_tg_id, sent_mid)
    return web.json_response({"ok": bool(sent_mid), "sent_to": target_tg_id})
