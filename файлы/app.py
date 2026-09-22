from flask import Flask, jsonify, render_template, request, Response
import time, math, json, sqlite3, os
from datetime import datetime

app = Flask(__name__)

# === КОНСТАНТЫ ===
MAX_GAS_FLOW = 120.0
MAX_LIQ_FLOW = 45.0
NOM_FG, NOM_FL = 84.0, 31.5
NOM_P,  NOM_T  = 1.25, 37.1
NOM_L,  NOM_C  = 56.8, 0.05
NOM_EFF, NOM_dP = 95.0, 0.44

# Параметры модели Кремсера
N_TRAYS    = 8        # число теоретических ступеней
HENRY_M0   = 0.40     # константа Генри при T0=298 K, P=1 бар
HENRY_dH_R = 1500.0   # приведённая теплота растворения, K
T0_KELVIN  = 298.0    # стандартная температура, K

# Молярные массы и условия пересчёта
MOLAR_GAS_VOL = 22.4  # объём моля идеального газа при норм. усл., м³/кмоль
MOLAR_LIQ_MW  = 18.0  # молярная масса абсорбента (вода), кг/кмоль

# Путь к файлу базы данных
DB_PATH = "kolonna.db"
PAUSED_ELAPSED = 0.0
ZNACH = {"FG": NOM_FG, "FL": NOM_FL, "P1": NOM_P, "T1": NOM_T,
         "L1": NOM_L, "C": NOM_C, "eff": NOM_EFF, "dp": NOM_dP}
KLAPANS = {"gas": 70, "liq": 70}
START_TIME = time.time()

EMERGENCY_STOP = False
MONITORING = True
EMERGENCY_WRITE = False
EVENTS = []
ALARMS = []
WARNED = {"P1_high": False, "L1_high": False, "C_high": False}

# Счётчик для прореживания записей истории параметров (раз в N тиков)
_save_counter = 0
SAVE_EVERY_N_TICKS = 1   # 1 — каждую секунду; поставь 5 если хочешь реже


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,category TEXT NOT NULL,
            tag TEXT,description TEXT,FG REAL, FL REAL,
            P REAL, L REAL,T REAL, C REAL, dp REAL,
            eff REAL, value TEXT, status TEXT NOT NULL)""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS params (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            FG REAL, FL REAL, P REAL, L REAL,
            T REAL, C REAL, dp REAL, eff REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alarms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            category TEXT NOT NULL,
            tag TEXT,
            description TEXT,
            operator TEXT,
            value TEXT,
            status TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def save_event_to_db(event):
    """Запись события в events."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""INSERT INTO events
            (timestamp, category, tag, description,
            FG, FL, P, L, T, C, dp, eff, value, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (event["timestamp"], event["category"], event["tag"],
            event["description"], event["FG"], event["FL"],
            event["P"], event["L"], event["T"], event["C"],
            event["dp"], event["eff"], event["value"], event["status"]))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB error (event): {e}")
        
def save_alarm_to_db(alarm):
    """Запись аварии в таблицу alarms."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            INSERT INTO alarms
            (timestamp, category, tag, description, operator, value, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (alarm["timestamp"], alarm["category"], alarm["tag"],
              alarm["description"], alarm.get("operator", ""),
              alarm["value"], alarm["status"]))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB error (alarm): {e}")


def save_params_to_db():
    """Запись текущих параметров в таблицу params."""
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            INSERT INTO params
            (timestamp, FG, FL, P, L, T, C, dp, eff)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ts, ZNACH["FG"], ZNACH["FL"], ZNACH["P1"],
              ZNACH["L1"], ZNACH["T1"], ZNACH["C"],
              ZNACH["dp"], ZNACH["eff"]))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB error (params): {e}")


def restore_state_from_db():
    """Восстанавливает последнее состояние при запуске."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""SELECT timestamp, category, tag, description,
                   FG, FL, P, L, T, C, dp, eff, value, status
                    FROM events ORDER BY id DESC LIMIT 100""")
        rows = cur.fetchall()
        for r in reversed(rows):
            EVENTS.append({
                "id": len(EVENTS) + 1,"timestamp": r[0],
                "category": r[1], "tag": r[2],"description": r[3],
                "FG": r[4], "FL": r[5],"P": r[6], "L": r[7],
                "T": r[8], "C": r[9],"dp": r[10], "eff": r[11],
                "value": r[12], "status": r[13]})
        cur.execute("""SELECT timestamp, category, tag, description,
                    operator, value, status FROM alarms ORDER BY id DESC LIMIT 100""")
        rows = cur.fetchall()
        for r in reversed(rows):
            ALARMS.append({
                "id": len(ALARMS) + 1,"timestamp": r[0], "category": r[1],
                "tag": r[2],"description": r[3], "operator": r[4],
                "value": r[5], "status": r[6]})
        conn.close()
    except Exception as e:
        print(f"DB restore error: {e}")


# === УТИЛИТЫ ===
def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def move(cur, tgt, k):
    return cur + (tgt - cur) * k

def log_event(category, tag, description, value="", status="INFO"):
    event = {
        "id": len(EVENTS) + 1,
        "timestamp": time.strftime("%H:%M:%S"),
        "category": category, "tag": tag, "description": description,
        "FG": round(ZNACH["FG"], 1), "FL": round(ZNACH["FL"], 1),
        "P": round(ZNACH["P1"], 2), "L": round(ZNACH["L1"], 1),
        "T": round(ZNACH["T1"], 1), "C": round(ZNACH["C"], 3),
        "dp": round(ZNACH["dp"], 3), "eff": round(ZNACH["eff"], 1),
        "value": value, "status": status}
    EVENTS.append(event)
    save_event_to_db(event)
    if len(EVENTS) > 5000:
        EVENTS.pop(0)


# === ФИЗИКА ===
def calc_henry(T_c, P_bar):
    """Константа фазового равновесия (закон Генри).
    Растёт с температурой, падает с давлением."""
    Tk = T_c + 273.15
    return HENRY_M0 * math.exp(HENRY_dH_R * (1.0/T0_KELVIN - 1.0/Tk)) / P_bar


def calc_flood(FG, FL):
    """Фактор захлёбывания по корреляции Леваса."""
    return clamp(FG/MAX_GAS_FLOW*0.85 + FL/MAX_LIQ_FLOW*0.15, 0, 1.5)


def calc_concentration(FG, FL, L, ff, disturbance, T, P, dp):
    """Концентрация целевого компонента на выходе.

    Базовая физика — уравнение Кремсера:
        C_out = C_in * (A - 1) / (A^(N+1) - 1)
    где A = L_mol / (m * G_mol), m = m(T, P) по Генри.

    Поверх Кремсера накладываются феноменологические поправки
    на гидродинамические нештатные явления, которые Кремсер не описывает:
    рост перепада давления, захлёбывание, отклонения уровня в кубе.
    """
    FG = max(FG, 1.0)
    FL = max(FL, 0.1)

    
    G_mol  = FG / MOLAR_GAS_VOL     
    L_mol  = FL / MOLAR_LIQ_MW      # кмоль/ч
    m = calc_henry(T, P)
    A = L_mol / (m * G_mol)

    if abs(A - 1.0) < 1e-6:
        eta_base = N_TRAYS / (N_TRAYS + 1)
    else:
        eta_base = (A**(N_TRAYS+1) - A) / (A**(N_TRAYS+1) - 1)
    eta_base = clamp(eta_base, 0.0, 0.99)
    C_base = 1.0 - eta_base

    # Феноменологические поправки на нештатные явления
    # Подобраны так, чтобы аварийный сценарий доводил C до ~0.4-0.5
    dp_effect    = (dp - 0.75) * 0.25 if dp > 0.75 else 0.0
    flood_effect = (ff - 0.85) * 0.45 if ff > 0.85 else 0.0
    level_effect = 0.0
    if L > 80: level_effect = (L - 80) * 0.006
    if L < 30: level_effect = (30 - L) * 0.006

    C = C_base + dp_effect + flood_effect + level_effect + disturbance
    return clamp(C, 0.02, 0.85)


def calc_effictiv(C, FG=None):
    if FG is not None and FG < 5.0:
        return clamp(FG / 5.0 * (1.0 - C) * 100.0, 0, 98)
    return clamp((1.0 - C) * 100.0, 0, 98)


def calc_pressure(FG, FL, ff, disturbance):
    dp = 0.008 * (FG/10)**1.8 + 0.0015*FL
    if ff > 0.75:
        dp += math.exp((ff - 0.75)*4) * 0.05
    P = 1.0 + dp + disturbance
    return clamp(P, 0.8, 3.0), clamp(dp, 0, 2.0)


def calc_level(FL, ff, disturbance):
    base = 35.0 + 0.72 * FL
    if ff > 0.85:
        base += (ff - 0.85) * 55
    return clamp(base + disturbance, 10, 95)


def calc_temperature(FG, FL, eff, disturbance):
    q_abs = 0.028 * FG * (eff / 100.0)
    if FG > 100:
        q_abs += (FG - 100) * 0.08
    q_cool = 0.020 * FL
    return clamp(36.0 + q_abs - q_cool + disturbance, 20, 100)


# === ОСНОВНОЙ ЦИКЛ ===
def process():
    global EMERGENCY_STOP, EMERGENCY_WRITE, _save_counter
    if not MONITORING: return
    timestart = time.time() - START_TIME
    gas_open = KLAPANS["gas"] / 100.0
    liq_open = KLAPANS["liq"] / 100.0

    if EMERGENCY_STOP:
        ZNACH["FG"]  = move(ZNACH["FG"], 0, 0.20)
        ZNACH["FL"]  = move(ZNACH["FL"], 0, 0.20)
        ZNACH["P1"]  = move(ZNACH["P1"], 1.0, 0.20)
        ZNACH["dp"]  = move(ZNACH["dp"], 0, 0.20)
        ZNACH["L1"]  = move(ZNACH["L1"], 0, 0.10)
        ZNACH["T1"]  = move(ZNACH["T1"], 20.0, 0.05)
        ZNACH["C"]   = move(ZNACH["C"], 0.0, 0.15)
        ZNACH["eff"] = calc_effictiv(ZNACH["C"], ZNACH["FG"])
        if not EMERGENCY_WRITE:
            log_event("АВАРИЯ", "ESD", "Колонна переведена в аварийный режим",
                f"P={round(ZNACH['P1'],2)} L={round(ZNACH['L1'],1)} T={round(ZNACH['T1'],1)} C={round(ZNACH['C'],3)}",
                "EMERGENCY")
            EMERGENCY_WRITE = True
        _save_periodic()
        return

    emergency_mode = timestart > 55 and not EMERGENCY_STOP
    pressure_disturbance = level_disturbance = conc_disturbance = temp_disturbance = gas_disturbance = 0.0
    if timestart > 5:
        base_pressure = 0.42 * clamp((timestart - 5) / 15, 0, 1)
        pressure_disturbance = max(0, base_pressure - (1.0 - gas_open) * 0.65)
    if timestart > 25:
        base_level = 24 * clamp((timestart - 25) / 10, 0, 1)
        level_disturbance = max(0, base_level - (1.0 - liq_open) * 18)
    if timestart > 30:
        base_gas = 22 * clamp((timestart - 30) / 15, 0, 1)
        gas_disturbance = max(0, base_gas - (1.0 - gas_open) * 16)
    if timestart > 50:
        conc_disturbance = 0.12 * clamp((timestart - 50) / 5, 0, 1)
    if emergency_mode:
        koefvozm = clamp((timestart - 55) / 15, 0, 1)
        pressure_disturbance = 0.85 * koefvozm
        level_disturbance = 38.0 * koefvozm
        conc_disturbance = 0.22 * koefvozm
        temp_disturbance = 22.0 * koefvozm

    target_FG = MAX_GAS_FLOW * gas_open + gas_disturbance
    target_FL = MAX_LIQ_FLOW * liq_open
    if emergency_mode:
        koefvozm = clamp((timestart - 55) / 15, 0, 1)
        target_FG += 65 * koefvozm
        target_FL += 25 * koefvozm
    speedrashod = 0.18 if emergency_mode else 0.10
    ZNACH["FG"] = move(ZNACH["FG"], target_FG, speedrashod)
    ZNACH["FL"] = move(ZNACH["FL"], target_FL, speedrashod)

    ff = calc_flood(ZNACH["FG"], ZNACH["FL"])
    target_P, target_dp = calc_pressure(ZNACH["FG"], ZNACH["FL"], ff, pressure_disturbance)
    p_speed  = 0.35 if gas_open < 0.55 else 0.12
    dp_speed = 0.03 if emergency_mode else 0.10
    ZNACH["P1"] = move(ZNACH["P1"], target_P,  p_speed)
    ZNACH["dp"] = move(ZNACH["dp"], target_dp, dp_speed)

    target_L = calc_level(ZNACH["FL"], ff, level_disturbance)
    l_speed = 0.20 if liq_open < 0.65 else 0.08
    ZNACH["L1"] = move(ZNACH["L1"], target_L, l_speed)

    target_T = calc_temperature(ZNACH["FG"], ZNACH["FL"], ZNACH["eff"], temp_disturbance)
    ZNACH["T1"] = move(ZNACH["T1"], target_T, 0.04)

    target_C = calc_concentration(ZNACH["FG"], ZNACH["FL"], ZNACH["L1"], ff,
                                  conc_disturbance, ZNACH["T1"], ZNACH["P1"], ZNACH["dp"])
    ZNACH["C"]   = move(ZNACH["C"], target_C, 0.08)
    ZNACH["eff"] = calc_effictiv(ZNACH["C"], ZNACH["FG"])

    ZNACH["FG"]  = clamp(ZNACH["FG"], 0, 180)
    ZNACH["FL"]  = clamp(ZNACH["FL"], 0, 80)
    ZNACH["P1"]  = clamp(ZNACH["P1"], 0.8, 3.0)
    ZNACH["T1"]  = clamp(ZNACH["T1"], 20, 100)
    ZNACH["L1"]  = clamp(ZNACH["L1"], 0, 95)
    ZNACH["C"]   = clamp(ZNACH["C"], 0.02, 1.0)
    ZNACH["eff"] = clamp(ZNACH["eff"], 0, 98)
    ZNACH["dp"]  = clamp(ZNACH["dp"], 0, 2.0)

    status = "INFO"
    if ZNACH["P1"] > 1.9 or ZNACH["L1"] > 85 or ZNACH["C"] > 0.40:
        status = "ALARM"
    elif ZNACH["P1"] > 1.6 or ZNACH["L1"] > 75 or ZNACH["C"] > 0.20:
        status = "WARNING"
    log_event("ДАННЫЕ", "ALL", "Параметры колонны",
        f"FG={round(ZNACH['FG'],1)} FL={round(ZNACH['FL'],1)} P={round(ZNACH['P1'],2)} "
        f"L={round(ZNACH['L1'],1)} T={round(ZNACH['T1'],1)} C={round(ZNACH['C'],3)} "
        f"Эфф={round(ZNACH['eff'],1)}% ΔP={round(ZNACH['dp'],3)}",
        status)

    if ZNACH["P1"] > 1.6 and not WARNED["P1_high"]:
        WARNED["P1_high"] = True
        log_event("ПРЕДЕЛЫ", "P1", "Давление превысило допустимый предел",
                  f"{round(ZNACH['P1'],2)} бар", "WARNING")
    elif ZNACH["P1"] <= 1.6:
        WARNED["P1_high"] = False

    if ZNACH["L1"] > 80 and not WARNED["L1_high"]:
        WARNED["L1_high"] = True
        log_event("ПРЕДЕЛЫ", "L1", "Уровень жидкости превысил норму",
                  f"{round(ZNACH['L1'],1)} %", "WARNING")
    elif ZNACH["L1"] <= 80:
        WARNED["L1_high"] = False

    if ZNACH["C"] > 0.20 and not WARNED["C_high"]:
        WARNED["C_high"] = True
        log_event("ПРЕДЕЛЫ", "C", "Концентрация на выходе превысила норму",
                  f"{round(ZNACH['C'],3)}", "ALARM")
    elif ZNACH["C"] <= 0.20:
        WARNED["C_high"] = False

    _save_periodic()


def _save_periodic():
    """Сохраняет параметры в БД с заданной периодичностью."""
    global _save_counter
    _save_counter += 1
    if _save_counter >= SAVE_EVERY_N_TICKS:
        save_params_to_db()
        _save_counter = 0


# === ЦВЕТА СТАТУСОВ ДЛЯ КОЛОННЫ ===
def status_T(T):
    if T > 60: return "red"
    if T > 45: return "yellow"
    return "green"

def status_P(P):
    if P > 1.9: return "red"
    if P > 1.6: return "yellow"
    return "green"

def status_L(L):
    if L < 20 or L > 85: return "red"
    if L < 30 or L > 75: return "yellow"
    return "green"

# === МАРШРУТЫ: страницы ===
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/log")
def log():
    return render_template("log.html")

@app.route("/alarms")
def alarms():
    return render_template("alarms.html")

# === МАРШРУТЫ: данные ===
@app.route("/data")
def data():
    process()
    return jsonify({
        "time":   time.strftime("%H:%M:%S"),
        "params": {"T1": round(ZNACH["T1"], 1),
                   "P1": round(ZNACH["P1"], 2),
                   "L1": round(ZNACH["L1"], 1)},
        "pipe":   {"FG":  round(ZNACH["FG"], 1),
                   "FL":  round(ZNACH["FL"], 1),
                   "C":   round(ZNACH["C"], 3),
                   "eff": round(ZNACH["eff"], 1),
                   "dp":  round(ZNACH["dp"], 3)},
        "status": {"T1": status_T(ZNACH["T1"]),
                   "P1": status_P(ZNACH["P1"]),
                   "L1": status_L(ZNACH["L1"]),
                   "emergency": EMERGENCY_STOP}})

# === МАРШРУТЫ: управление клапанами ===
@app.route("/gas", methods=["POST"])
def gas():
    v = clamp(float(request.json["v"]), 0, 100)
    KLAPANS["gas"] = v
    log_event("КЛАПАНЫ", "FG-CV", "Оператор изменил подачу газа", f"{v}%", "INFO")
    return "ok"
@app.route("/liq", methods=["POST"])
def liq():
    v = clamp(float(request.json["v"]), 0, 100)
    KLAPANS["liq"] = v
    log_event("КЛАПАНЫ", "FL-CV", "Оператор изменил подачу жидкости", f"{v}%", "INFO")
    return "ok"

# === МАРШРУТЫ: режимы работы ===
@app.route("/emergency", methods=["POST"])
def emergency():
    global EMERGENCY_STOP
    EMERGENCY_STOP = True
    body = request.get_json() or {}
    operator = body.get("operator", "Оператор")
    val = f"P={round(ZNACH['P1'],2)} L={round(ZNACH['L1'],1)} T={round(ZNACH['T1'],1)} C={round(ZNACH['C'],3)}"
    alarm = {
        "id": len(ALARMS) + 1,
        "timestamp": time.strftime("%H:%M:%S"),
        "category": "АВАРИЯ", "tag": "ESD",
        "description": "Аварийная остановка активирована",
        "operator": operator, "value": val, "status": "EMERGENCY"}
    ALARMS.append(alarm)
    save_alarm_to_db(alarm)
    log_event("АВАРИЯ", "ESD", "Активирована аварийная остановка", val, "EMERGENCY")
    return "ok"

@app.route("/start", methods=["POST"])
def start():
    global MONITORING, START_TIME
    MONITORING = True
    START_TIME = time.time() - PAUSED_ELAPSED
    log_event("СИСТЕМА", "START", "Мониторинг запущен", "", "INFO")
    return "ok"

@app.route("/pause", methods=["POST"])
def pause():
    global MONITORING, PAUSED_ELAPSED
    PAUSED_ELAPSED = time.time() - START_TIME
    MONITORING = False
    log_event("СИСТЕМА", "PAUSE", "Мониторинг приостановлен", "", "INFO")
    return "ok"

@app.route("/reset", methods=["POST"])
def reset():
    global EMERGENCY_STOP, START_TIME, EMERGENCY_WRITE
    EMERGENCY_STOP = False
    EMERGENCY_WRITE = False
    START_TIME = time.time()
    KLAPANS["gas"] = 70
    KLAPANS["liq"] = 70
    WARNED["P1_high"] = False
    WARNED["L1_high"] = False
    WARNED["C_high"]  = False
    log_event("СИСТЕМА", "RESET", "Система запущена после аварии", "", "RESET")
    return "ok"

# === МАРШРУТЫ: журналы ===
@app.route("/api/journal")
def api_journal():
    return Response(json.dumps(EVENTS, ensure_ascii=False), mimetype='application/json')

@app.route("/api/alarms")
def api_alarms():
    return Response(json.dumps(ALARMS, ensure_ascii=False), mimetype='application/json')

@app.route("/api/clear_journal", methods=["POST"])
def clear_journal():
    EVENTS.clear()
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM events")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB error (clear events): {e}")
    return "ok"

@app.route("/api/clear_alarms", methods=["POST"])
def clear_alarms():
    ALARMS.clear()
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM alarms")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB error (clear alarms): {e}")
    return "ok"

# === ЗАПУСК ===
if __name__ == "__main__":
    init_db()
    restore_state_from_db()
    log_event("СИСТЕМА", "INIT", "Запуск системы мониторинга", "", "INFO")
    app.run(host="0.0.0.0", port=5001, debug=True, use_reloader=False)
