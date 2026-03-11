from flask import Flask, render_template, jsonify, request as req
from pylogix import PLC
from database import engine, criar_tabelas, StatusMaquina, HistoricoMissoes, AlarmesEvento, DB_URL
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timedelta
from threading import Thread, Lock
from math import ceil
import psycopg2, psycopg2.extras, time

# ─── Config ───────────────────────────────────────────────────────────
PLC_A1_IP       = "192.168.100.1"
PLC_A2_IP       = "192.168.100.10"
PLC_SLOT        = 0
INTERVALO_PLC   = 0.5   # leitura por PLC (paralelo)
INTERVALO_PROC  = 1.0   # ciclo de processamento
TEMPO_ALVO_SEG  = 90.0  # referência de performance OEE

# ─── Tags ─────────────────────────────────────────────────────────────
TAGS_A1 = [
    "PViewMemories_B60[0]", "MaquinaSemDefeitos", "Grafcet_B14[1].1",
    "Nivel2DataMsg_N22[0]",  # <- ADICIONADO: ID da Mensagem
    "Nivel2DataMsg_N22[3]",  "Nivel2DataMsg_N22[6]",  "Nivel2DataMsg_N22[7]",
    "Nivel2DataMsg_N22[8]",  "Nivel2DataMsg_N22[10]", "Nivel2DataMsg_N22[11]",
    "Nivel2DataMsg_N22[12]", "Nivel2DataMsg_N22[21]", "Nivel2DataMsg_N22[22]",
]
TAGS_ALARMES_A1 = [f"Faults_B18[{i}]" for i in range(10)]
TAGS_A2 = (
    ["Status_B3[0]", "Status_B3[3]", "Coluna_Atual", "Andar_Atual"]
    + [f"Auxiliar_N23[{i}]" for i in [3, 6, 7, 8, 10, 11, 12, 30]]
)

# ─── LiveState: memória compartilhada entre threads ───────────────────
class LiveState:
    def __init__(self):
        self._lock       = Lock()
        self.a1          = {}
        self.a2          = {}
        self.ts_a1       = None
        self.ts_a2       = None
        self.ok_a1       = False
        self.ok_a2       = False
        # Status derivados (atualizados pelo processor)
        self.modo_producao   = False
        self.modo_manutencao = False
        self.sem_defeitos    = True
        self.missao_ativa    = False
        self.posicao_col     = 0
        self.posicao_andar   = 0
        self.missao_atual    = None  # {tipo, origem, destino, inicio_ts}

    def set_a1(self, d):
        with self._lock:
            self.a1 = d; self.ts_a1 = datetime.now(); self.ok_a1 = bool(d)

    def set_a2(self, d):
        with self._lock:
            self.a2 = d; self.ts_a2 = datetime.now(); self.ok_a2 = bool(d)

    def fail_a1(self):
        with self._lock: self.ok_a1 = False

    def fail_a2(self):
        with self._lock: self.ok_a2 = False

    def set_status(self, **kw):
        with self._lock:
            for k, v in kw.items():
                setattr(self, k, v)

    def snapshot(self):
        with self._lock:
            return {
                "a1": dict(self.a1), "a2": dict(self.a2),
                "ts_a1": self.ts_a1, "ts_a2": self.ts_a2,
                "ok_a1": self.ok_a1, "ok_a2": self.ok_a2,
                "modo_producao":   self.modo_producao,
                "modo_manutencao": self.modo_manutencao,
                "sem_defeitos":    self.sem_defeitos,
                "missao_ativa":    self.missao_ativa,
                "posicao_col":     self.posicao_col,
                "posicao_andar":   self.posicao_andar,
                "missao_atual":    dict(self.missao_atual) if self.missao_atual else None,
            }

LIVE = LiveState()

# ─── Utilitários ──────────────────────────────────────────────────────
def norm(leituras):
    if not isinstance(leituras, list): leituras = [leituras]
    return {r.TagName: r.Value for r in leituras if r.Status == "Success"}

def bit(v, n):
    try: return bool(int(v or 0) & (1 << n))
    except: return False

def si(v, d=0):
    try: return int(v) if v is not None else d
    except: return d

def loc(lado, col, andar):
    return f"L{si(lado)}-C{si(col)}-A{si(andar)}"

def tipo_txt(t):
    return {1: "Carregamento", 2: "Descarregamento", 3: "Transferencia"}.get(t, f"Tipo_{t}")

def snap_missao(a1, a2):
    tipo = si(a2.get("Auxiliar_N23[30]")) or si(a2.get("Auxiliar_N23[3]")) or si(a1.get("Nivel2DataMsg_N22[3]"))
    def g(k2, k1):
        v = si(a2.get(k2))
        return v if v != 0 else si(a1.get(k1))
    return {
        "tipo":       tipo,
        "orig_lado":  g("Auxiliar_N23[6]",  "Nivel2DataMsg_N22[6]"),
        "orig_col":   g("Auxiliar_N23[7]",  "Nivel2DataMsg_N22[7]"),
        "orig_andar": g("Auxiliar_N23[8]",  "Nivel2DataMsg_N22[8]"),
        "dest_lado":  g("Auxiliar_N23[10]", "Nivel2DataMsg_N22[10]"),
        "dest_col":   g("Auxiliar_N23[11]", "Nivel2DataMsg_N22[11]"),
        "dest_andar": g("Auxiliar_N23[12]", "Nivel2DataMsg_N22[12]"),
        "status_int": si(a1.get("Nivel2DataMsg_N22[21]")),
        "cr":         si(a1.get("Nivel2DataMsg_N22[22]")),
    }

def processar_alarmes(session, palavras):
    rows = session.execute(
        text("SELECT palavra, valor FROM alarmes_estado_anterior ORDER BY palavra")
    ).fetchall()
    if not rows:
        for i in range(len(palavras)):
            session.execute(
                text("INSERT INTO alarmes_estado_anterior (palavra,valor,atualizado) VALUES (:p,0,NOW()) ON CONFLICT(palavra) DO NOTHING"),
                {"p": i}
            )
        session.flush()
        rows = session.execute(
            text("SELECT palavra, valor FROM alarmes_estado_anterior ORDER BY palavra")
        ).fetchall()
    eventos = []
    for idx, atual in enumerate(palavras):
        anterior = si(rows[idx].valor) if idx < len(rows) else 0
        diff = anterior ^ atual
        if not diff: continue
        for b in range(32):
            if diff & (1 << b):
                ev = AlarmesEvento(
                    tipo="ATIVADO" if (atual & (1 << b)) else "DESATIVADO",
                    numero=idx * 32 + b, descricao=f"Falha Faults_B18[{idx}].{b}",
                    palavra=idx, bit=b,
                )
                session.add(ev); eventos.append(ev)
        session.execute(
            text("INSERT INTO alarmes_estado_anterior (palavra,valor,atualizado) VALUES (:p,:v,NOW()) ON CONFLICT(palavra) DO UPDATE SET valor=EXCLUDED.valor,atualizado=NOW()"),
            {"p": idx, "v": atual}
        )
    return eventos

# ─── Thread A1 ────────────────────────────────────────────────────────
def thread_a1():
    print(f"[A1] Thread iniciada → {PLC_A1_IP}")
    while True:
        try:
            with PLC() as c:
                c.IPAddress = PLC_A1_IP; c.ProcessorSlot = PLC_SLOT; c.SocketTimeout = 2.0
                print("[A1] Conectado")
                while True:
                    t0 = time.time()
                    d = norm(c.Read(TAGS_A1 + TAGS_ALARMES_A1))
                    LIVE.set_a1(d) if d else LIVE.fail_a1()
                    time.sleep(max(0, INTERVALO_PLC - (time.time() - t0)))
        except Exception as e:
            print(f"[A1] Erro: {e} — reconectando em 3s")
            LIVE.fail_a1(); time.sleep(3)

# ─── Thread A2 ────────────────────────────────────────────────────────
def thread_a2():
    print(f"[A2] Thread iniciada → {PLC_A2_IP}")
    while True:
        try:
            with PLC() as c:
                c.IPAddress = PLC_A2_IP; c.ProcessorSlot = PLC_SLOT; c.SocketTimeout = 2.0
                print("[A2] Conectado")
                while True:
                    t0 = time.time()
                    d = norm(c.Read(TAGS_A2))
                    LIVE.set_a2(d) if d else LIVE.fail_a2()
                    time.sleep(max(0, INTERVALO_PLC - (time.time() - t0)))
        except Exception as e:
            print(f"[A2] Erro: {e} — reconectando em 3s")
            LIVE.fail_a2(); time.sleep(3)

# ─── Thread Processor (gravação no banco) ─────────────────────────────
def thread_processor():
    ultimo_cr     = None
    ultima_missao = False
    cache_inicio  = None

    while True:
        t0 = time.time(); agora = datetime.now()
        snap = LIVE.snapshot()
        a1, a2 = snap["a1"], snap["a2"]

        if not snap["ok_a1"] and not snap["ok_a2"]:
            time.sleep(INTERVALO_PROC); continue

        sb3_0 = si(a2.get("Status_B3[0]") or a1.get("Status_B3[0]"))
        sb3_3 = si(a2.get("Status_B3[3]"))

        modo_prod    = bit(sb3_0, 5)
        modo_manut   = bit(sb3_0, 2)
        missao_ativa = bit(sb3_3, 0)
        maq_ok       = bool(a1.get("MaquinaSemDefeitos", True))
        coluna       = si(a2.get("Coluna_Atual"))
        andar        = si(a2.get("Andar_Atual"))

        LIVE.set_status(
            modo_producao=modo_prod, modo_manutencao=modo_manut,
            sem_defeitos=maq_ok, missao_ativa=missao_ativa,
            posicao_col=coluna, posicao_andar=andar,
        )

        try:
            with Session(engine) as session:
                session.add(StatusMaquina(
                    timestamp=agora, modo_producao=modo_prod,
                    modo_manutencao=modo_manut, sem_defeitos=maq_ok,
                    missao_em_andamento=missao_ativa,
                    posicao_translacao=coluna, posicao_elevacao=andar,
                ))

                # Início de missão (borda de subida)
                if missao_ativa and not ultima_missao:
                    cache_inicio = snap_missao(a1, a2)
                    orig = loc(cache_inicio["orig_lado"], cache_inicio["orig_col"],  cache_inicio["orig_andar"])
                    dest = loc(cache_inicio["dest_lado"], cache_inicio["dest_col"],  cache_inicio["dest_andar"])
                    tipo = tipo_txt(cache_inicio["tipo"])
                    LIVE.set_status(missao_atual={"tipo": tipo, "origem": orig, "destino": dest, "inicio_ts": agora})
                    session.add(HistoricoMissoes(
                        timestamp_inicio=agora, ciclo_completo=False,
                        tipo_missao=tipo, origem=orig, destino=dest, status_fim="EM_ANDAMENTO",
                    ))
                    print(f"[{agora.strftime('%H:%M:%S')}] >>> INÍCIO: {tipo} | {orig} → {dest}")

                # Fim de missão (mudança de CR)
                cr = a1.get("Nivel2DataMsg_N22[22]")
                msg_id = si(a1.get("Nivel2DataMsg_N22[0]"))

                if cr is not None and ultimo_cr is not None and si(cr) != si(ultimo_cr):
                    # TRAVA KEEP-ALIVE: Garante que é a MSG 702 e é a PRIMEIRA notificação dessa missão (cr <= 1)
                    if msg_id == 702 and si(cr) <= 1:
                        df   = snap_missao(a1, a2)
                        base = cache_inicio if cache_inicio else df
                        stxt = "OK" if si(df["status_int"]) == 1 else "FALHA"
                        ma = (
                            session.query(HistoricoMissoes)
                            .filter_by(ciclo_completo=False)
                            .order_by(HistoricoMissoes.timestamp_inicio.desc())
                            .first()
                        )
                        if not ma:
                            ma = HistoricoMissoes(timestamp_inicio=agora, ciclo_completo=False)
                            session.add(ma); session.flush()
                        ma.timestamp_fim    = agora
                        ma.duracao_segundos = max(0.0, (agora - ma.timestamp_inicio).total_seconds())
                        ma.tipo_missao      = tipo_txt(base["tipo"])
                        ma.origem           = loc(base["orig_lado"], base["orig_col"], base["orig_andar"])
                        ma.destino          = loc(base["dest_lado"], base["dest_col"], base["dest_andar"])
                        ma.status_fim       = stxt; ma.ciclo_completo = True
                        LIVE.set_status(missao_atual=None)
                        print(f"[{agora.strftime('%H:%M:%S')}] <<< FIM: {ma.tipo_missao} | "
                              f"{ma.origem}→{ma.destino} | {ma.duracao_segundos:.1f}s | {stxt}")
                        cache_inicio = None
                    else:
                        # Reenvio do keep-alive (cr > 1) ou outro tipo de msg. Ignoramos o salvamento.
                        pass

                ultima_missao = missao_ativa; ultimo_cr = cr
                palavras = [si(a1.get(f"Faults_B18[{i}]")) for i in range(10)]
                eventos  = processar_alarmes(session, palavras)
                session.commit()

                modo_str = "PROD" if modo_prod else ("MANUT" if modo_manut else "PARADO")
                print(f"[{agora.strftime('%H:%M:%S')}] {modo_str} | Col:{coluna} And:{andar} | "
                      f"Missão:{'SIM' if missao_ativa else 'NAO'} | CR:{si(cr)} | "
                      f"A1:{'OK' if snap['ok_a1'] else 'ERR'} A2:{'OK' if snap['ok_a2'] else 'ERR'}")
        except Exception as e:
            print(f"[PROC] Erro DB: {e}")

        time.sleep(max(0, INTERVALO_PROC - (time.time() - t0)))

# ─── Flask ────────────────────────────────────────────────────────────
app = Flask(__name__)

def db():
    conn = psycopg2.connect(DB_URL)
    return conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/api/live")
def api_live():
    """Dados em tempo real — lê do LiveState em memória, sem query ao banco."""
    snap = LIVE.snapshot(); agora = datetime.now()

    def age(ts): return (agora - ts).total_seconds() if ts else 9999
    comm_a1 = snap["ok_a1"] and age(snap["ts_a1"]) < 3
    comm_a2 = snap["ok_a2"] and age(snap["ts_a2"]) < 3

    ma = snap["missao_atual"]
    if ma and snap["missao_ativa"]:
        missao = {
            "ativa": True, "tipo": ma.get("tipo", "-"),
            "origem": ma.get("origem", "-"), "destino": ma.get("destino", "-"),
            "inicio": ma["inicio_ts"].strftime("%H:%M:%S"),
            "duracao_atual": max(0, int((agora - ma["inicio_ts"]).total_seconds())),
        }
    else:
        missao = {"ativa": False, "tipo": "-", "origem": "-", "destino": "-", "inicio": "-", "duracao_atual": 0}

    conn, cur = db()
    cur.execute("""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN tipo_missao='Carregamento'    THEN 1 ELSE 0 END) AS entradas,
               SUM(CASE WHEN tipo_missao='Descarregamento' THEN 1 ELSE 0 END) AS saidas
        FROM historico_missoes WHERE ciclo_completo=true
    """)
    kpi = cur.fetchone(); cur.close(); conn.close()

    return jsonify({
        "agora":   agora.strftime("%d/%m/%Y %H:%M:%S"),
        "comm_a1": comm_a1, "comm_a2": comm_a2,
        "status": {
            "modo_producao":     snap["modo_producao"],
            "modo_manutencao":   snap["modo_manutencao"],
            "sem_defeitos":      snap["sem_defeitos"],
            "missao_em_andamento": snap["missao_ativa"],
            "posicao_translacao":  snap["posicao_col"],
            "posicao_elevacao":    snap["posicao_andar"],
        },
        "missao":    missao,
        "contadores": dict(kpi) if kpi else {"total": 0, "entradas": 0, "saidas": 0},
    })

@app.route("/api/missoes")
def api_missoes():
    page = max(1, int(req.args.get("page", 1))); per_page = 15
    conn, cur = db()
    
    # 1. Total para paginação (não alterado)
    cur.execute("SELECT COUNT(*) AS t FROM historico_missoes WHERE ciclo_completo=true")
    total = int(cur.fetchone()["t"] or 0); total_pages = ceil(total / per_page) if total else 1
    page = min(page, total_pages); offset = (page - 1) * per_page if total else 0
    
    # 2. Histórico da página atual (não alterado)
    cur.execute("""
        SELECT timestamp_inicio, timestamp_fim, duracao_segundos,
               tipo_missao, origem, destino, status_fim
        FROM historico_missoes WHERE ciclo_completo=true
        ORDER BY timestamp_fim DESC LIMIT %s OFFSET %s
    """, (per_page, offset))
    rows = cur.fetchall()
    
    # 3. Tempo médio LIMITADO à última hora (ALTERADO)
    cutoff = datetime.now() - timedelta(hours=1)
    cur.execute("""
        SELECT AVG(duracao_segundos) AS m 
        FROM historico_missoes 
        WHERE ciclo_completo=true 
          AND duracao_segundos>0 
          AND timestamp_fim >= %s
    """, (cutoff,))
    
    media = cur.fetchone(); cur.close(); conn.close()
    
    # 4. Formatação e retorno (não alterado)
    hist = []
    for r in rows:
        m = dict(r)
        m["timestamp_fim"]    = m["timestamp_fim"].strftime("%H:%M:%S") if m["timestamp_fim"] else "--:--:--"
        m["duracao_segundos"] = float(m["duracao_segundos"] or 0); hist.append(m)
        
    return jsonify({
        "page": page, "per_page": per_page, "total": total, "total_pages": total_pages,
        "tempo_medio": float(media["m"]) if media and media["m"] else 0.0,
        "historico": hist,
    })


@app.route("/api/oee")
def api_oee():
    TEMPO_ALVO_SEG = 30.0  # Ajustado para refletir a missão mais rápida (evita OEE > 100%)
    
    conn, cur = db()
    janela = "última hora"
    cutoff = datetime.now() - timedelta(hours=1)
    
    # Determina a janela dinamicamente
    for h, lbl in [(1, "última hora"), (8, "últimas 8h"), (24, "últimas 24h")]:
        ct = datetime.now() - timedelta(hours=h)
        cur.execute("SELECT COUNT(*) AS c FROM status_maquina WHERE timestamp >= %s", (ct,))
        if int(cur.fetchone()["c"] or 0) >= 15:
            cutoff, janela = ct, lbl
            break

    # Disponibilidade
    cur.execute("""
        SELECT COUNT(*) AS t,
               SUM(CASE WHEN modo_producao=true  THEN 1 ELSE 0 END) AS p,
               SUM(CASE WHEN modo_manutencao=true THEN 1 ELSE 0 END) AS mn
        FROM status_maquina WHERE timestamp >= %s
    """, (cutoff,))
    dr = cur.fetchone()
    ta = int(dr["t"] or 0)
    disp = 100.0 * (int(dr["p"] or 0) + int(dr["mn"] or 0)) / ta if ta > 0 else 0.0

    # Performance e Qualidade
    cur.execute("""
        SELECT COUNT(*) AS tm, 
               AVG(CASE WHEN duracao_segundos>0 THEN duracao_segundos END) AS ad,
               SUM(CASE WHEN status_fim='OK' THEN 1 ELSE 0 END) AS ok
        FROM historico_missoes 
        WHERE ciclo_completo=true AND timestamp_fim >= %s
    """, (cutoff,))
    mr = cur.fetchone()
    cur.close()
    conn.close()

    tm = int(mr["tm"] or 0)
    ad = float(mr["ad"] or 0)
    ok = int(mr["ok"] or 0)
    
    # DEBUG NO TERMINAL (agora vai aparecer porque você está rodando o script certo!)
    print(f"\n--- DEBUG OEE ({janela}) ---")
    print(f"Total Missões: {tm}")
    print(f"Tempo Médio Real: {ad:.2f}s | Alvo Ideal: {TEMPO_ALVO_SEG}s")

    perf = 0.0
    if ad > 0:
        perf = min(100.0, (TEMPO_ALVO_SEG / ad) * 100)
        
    qual = 100.0 * ok / tm if tm > 0 else 0.0
    
    oee_global = round((disp * perf * qual) / 10000, 1)
    
    print(f"Resultados -> D: {disp:.1f}% | P: {perf:.1f}% | Q: {qual:.1f}% | OEE: {oee_global}%")
    print("--------------------------\n")

    return jsonify({
        "janela": janela, 
        "disponibilidade": round(disp, 1),
        "performance": round(perf, 1), 
        "qualidade": round(qual, 1),
        "oee": oee_global,
    })

# ─── Startup ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    criar_tabelas()
    print("=" * 60)
    print("  TL14 — Processo Unificado (Coletor + Dashboard)")
    print(f"  A1: {PLC_A1_IP}   A2: {PLC_A2_IP}   Porta: 8050")
    print("=" * 60)
    Thread(target=thread_a1,        daemon=True, name="Reader-A1").start()
    Thread(target=thread_a2,        daemon=True, name="Reader-A2").start()
    time.sleep(1.5)
    Thread(target=thread_processor, daemon=True, name="Processor").start()
    app.run(host="0.0.0.0", port=8050, debug=False, threaded=True)
