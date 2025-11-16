# -*- coding: utf-8 -*-
"""
Ingestão CICIDS2017 -> RDF (.nt.gz), 1 arquivo por dia (para carregar em named graphs).
Suporta pruning na ingestão:
  --temporal 24h           (janela global; faz 1ª passada para achar t_max)
  --aggregate 60s          (agregação por bucket de tempo, src/dst/proto/dstPort)
  --topk 20                (backbone Top-K por src; 1ª passada: contar arestas; 2ª passada: filtrar)
  --sample 0.01            (amostragem uniforme p)
  --labels suspicious,ddos (manter só labels informadas; case-insensitive)

LABEL_LITERAL (ex: ex:label "DDoS").

Uso típico:
    python ingest_cicids2017.py --csv-root "CSVs/GeneratedLabelledFlows/TrafficLabelling" --baseline
    python ingest_cicids2017.py --csv-root ... --temporal 24h
    python ingest_cicids2017.py --csv-root ... --aggregate 60s
    python ingest_cicids2017.py --csv-root ... --topk 20
    python ingest_cicids2017.py --csv-root ... --sample 0.01
    python ingest_cicids2017.py --csv-root ... --labels suspicious,ddos,portscan
"""

import os, gzip, argparse, random
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import pandas as pd

# ======= CONFIG PADRÃO =======
CHUNK = 200_000
OUTPUT_DIR = "out_nt"
NAMESPACE_EX = "http://example.org/ns#"
NS_FLOW = "http://kg/cicids/flow/"
NS_IP   = "http://kg/cicids/ip/"

# Predicados/classes (mínimos)
P_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
EX_Flow = NAMESPACE_EX + "NetworkFlow"
EX_AggFlow = NAMESPACE_EX + "AggregatedFlow"
EX_IPAddress = NAMESPACE_EX + "IPAddress"
EX_ts = NAMESPACE_EX + "timestamp"
EX_tsEpoch = NAMESPACE_EX + "timestampEpoch"
EX_hasSrc = NAMESPACE_EX + "hasSourceAddress"
EX_hasDst = NAMESPACE_EX + "hasDestinationAddress"
EX_srcPort = NAMESPACE_EX + "srcPort"
EX_dstPort = NAMESPACE_EX + "dstPort"
EX_proto = NAMESPACE_EX + "protocol"
EX_label = NAMESPACE_EX + "label"
EX_duration = NAMESPACE_EX + "duration"
EX_count = NAMESPACE_EX + "count"
EX_sumBytes = NAMESPACE_EX + "sumBytes"
EX_bucketStart = NAMESPACE_EX + "bucketStart"
EX_firstSeen = NAMESPACE_EX + "firstSeen"
EX_lastSeen = NAMESPACE_EX + "lastSeen"

# ======= ARQUIVOS ESPERADOS =======
FILES = [
    "Monday-WorkingHours.pcap_ISCX.csv",
    "Tuesday-WorkingHours.pcap_ISCX.csv",
    "Wednesday-workingHours.pcap_ISCX.csv",
    "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv",
    "Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv",
    "Friday-WorkingHours-Morning.pcap_ISCX.csv",
    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv",
    "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv",
]

# ======= COLUNAS (com nomes "limpos", sem espaços nas pontas) =======
COL_FLOW_ID   = "Flow ID"
COL_SRC_IP    = "Source IP"
COL_SRC_PORT  = "Source Port"
COL_DST_IP    = "Destination IP"
COL_DST_PORT  = "Destination Port"
COL_PROTO     = "Protocol"
COL_TS        = "Timestamp"
COL_DUR       = "Flow Duration"
COL_FWD_BYTES = "Total Length of Fwd Packets"
COL_BWD_BYTES = "Total Length of Bwd Packets"
COL_LABEL     = "Label"

USECOLS = [
    COL_FLOW_ID, COL_SRC_IP, COL_SRC_PORT, COL_DST_IP, COL_DST_PORT, COL_PROTO,
    COL_TS, COL_DUR, COL_FWD_BYTES, COL_BWD_BYTES, COL_LABEL
]
USECOLS = [c.strip() for c in USECOLS]

# ======= HELPERS =======
def ensure_dir(d):
    if not os.path.exists(d):
        os.makedirs(d)

def ip_uri(ip):
    return NS_IP + str(ip).strip()

def flow_uri(day_key, idx):
    return f"{NS_FLOW}{day_key}/flow/{idx}"

def xsd_datetime(dt: datetime) -> str:
    # xsd:dateTime ISO 8601 em UTC (com 'Z')
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_timestamp(ts):
    s = str(ts).strip()
    # formatos mais comuns no CICIDS2017
    for fmt in (
        "%Y-%m-%d %H:%M:%S",    # 2017-07-04 17:02:14
        "%d/%m/%Y %H:%M:%S",    # 04/07/2017 17:02:14
        "%d/%m/%Y %I:%M:%S %p", # 04/07/2017 05:02:14 PM
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
    ):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        dt = pd.to_datetime(s, utc=True, errors="raise", dayfirst=True)
        if isinstance(dt, pd.Timestamp):
            return dt.to_pydatetime()
    except Exception:
        return None
    return None

def triple(s, p, o, literal=False, dtype=None, lang=None):
    # Gera uma linha N-Triples: <s> <p> <o> .
    if literal:
        lit = str(o).replace("\\", "\\\\").replace('"', '\\"')
        if dtype:
            return f"<{s}> <{p}> \"{lit}\"^^<{dtype}> .\n"
        elif lang:
            return f"<{s}> <{p}> \"{lit}\"@{lang} .\n"
        else:
            return f"<{s}> <{p}> \"{lit}\" .\n"
    else:
        return f"<{s}> <{p}> <{o}> .\n"

def write_triples(path_gz, lines_iter):
    with gzip.open(path_gz, "at", encoding="utf-8", newline="") as f:
        for line in lines_iter:
            f.write(line)

#def read_clean_csv_chunks(path, chunksize):
#    """Lê CSV em streaming e normaliza cabeçalho (strip)."""
#    for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=True):
#        chunk.columns = chunk.columns.str.strip()
#        yield chunk

def read_clean_csv_chunks(path, chunksize):
    """Lê CSV em streaming com fallback de encoding."""
    tried = False
    encodings_to_try = ["utf-8", "latin1"]
    for enc in encodings_to_try:
        try:
            for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=True, encoding=enc):
                chunk.columns = chunk.columns.str.strip()
                yield chunk
            return
        except UnicodeDecodeError:
            if not tried:
                print(f"[warn] Problema de encoding em {os.path.basename(path)}, tentando latin1...")
                tried = True
            continue
    raise SystemExit(f"[ERRO] Falha ao ler CSV com qualquer encoding: {path}")

def validate_columns_strict(path, required_cols):
    """Valida o header (strip) e exige todas as colunas."""
    df0 = pd.read_csv(path, nrows=0, low_memory=True)
    cols = [c.strip() for c in df0.columns.tolist()]
    missing = [c for c in required_cols if c not in cols]
    if missing:
        raise SystemExit(
            f"ERRO: CSV '{os.path.basename(path)}' está faltando colunas: {missing}\n"
            f"Colunas encontradas: {cols}"
        )
    return set(cols)

def first_pass_tmax(csv_root):
    tmax = None
    for fname in FILES:
        path = os.path.join(csv_root, fname)
        if not os.path.exists(path): 
            continue
        # valida header (strip + exigir colunas) para garantir COL_TS
        validate_columns_strict(path, USECOLS)
        for chunk in read_clean_csv_chunks(path, CHUNK):
            ts_series = chunk[COL_TS].dropna().astype(str)
            for ts in ts_series:
                dt = parse_timestamp(ts)
                if dt and ((tmax is None) or (dt > tmax)):
                    tmax = dt
    return tmax

def pass_count_edges(csv_root):
    """Conta arestas (src,dst) e por origem (src->dst->freq)."""
    counts = defaultdict(int)
    por_src = defaultdict(lambda: defaultdict(int))
    for fname in FILES:
        path = os.path.join(csv_root, fname)
        if not os.path.exists(path): 
            continue
        validate_columns_strict(path, USECOLS)
        for chunk in read_clean_csv_chunks(path, CHUNK):
            srcs = chunk[COL_SRC_IP].astype(str)
            dsts = chunk[COL_DST_IP].astype(str)
            for s, d in zip(srcs, dsts):
                counts[(s, d)] += 1
                por_src[s][d] += 1
    return counts, por_src

def build_topk_keep(por_src, k):
    """Para cada src, mantém os k destinos mais frequentes (set de (src,dst))."""
    keep = set()
    for src, dmap in por_src.items():
        top = sorted(dmap.items(), key=lambda kv: kv[1], reverse=True)[:k]
        for dst, _ in top:
            keep.add((src, dst))
    return keep

def day_key_from_name(name: str) -> str:
    base = name.split(".pcap_ISCX.csv")[0]
    return base.replace(" ", "_")

# ======= EMISSÃO DE FLOWS (baseline) =======
def emit_flow_triples(day_key, idx, row, tmax=None, temporal_td=None, labels_keep=None, sample_p=None):
    (
        flow_id, src_ip, src_port, dst_ip, dst_port, proto,
        ts_str, dur, fwd_bytes, bwd_bytes, label
    ) = row

    # pruning por labels
    if labels_keep and str(label).strip().lower() not in labels_keep:
        return

    # parse ts
    dt = parse_timestamp(str(ts_str))
    if not dt:
        return

    # pruning temporal global
    if temporal_td is not None and tmax is not None:
        if dt < (tmax - temporal_td):
            return

    # sampling
    if (sample_p is not None) and (random.random() >= sample_p):
        return

    s_flow = flow_uri(day_key, idx)
    s_ip = ip_uri(src_ip)
    d_ip = ip_uri(dst_ip)

    # tipagem
    yield triple(s_flow, P_TYPE, EX_Flow, literal=False)
    yield triple(s_ip, P_TYPE, EX_IPAddress, literal=False)
    yield triple(d_ip, P_TYPE, EX_IPAddress, literal=False)

    # relacionais
    yield triple(s_flow, EX_hasSrc, s_ip, literal=False)
    yield triple(s_flow, EX_hasDst, d_ip, literal=False)

    # timestamp (xsd:dateTime e epoch)
    yield triple(s_flow, EX_ts, xsd_datetime(dt), literal=True, dtype="http://www.w3.org/2001/XMLSchema#dateTime")
    yield triple(s_flow, EX_tsEpoch, int(dt.timestamp()), literal=True, dtype="http://www.w3.org/2001/XMLSchema#integer")

    # portas / protocolo
    if pd.notna(src_port):
        try:
            yield triple(s_flow, EX_srcPort, int(src_port), literal=True, dtype="http://www.w3.org/2001/XMLSchema#integer")
        except Exception:
            pass
    if pd.notna(dst_port):
        try:
            yield triple(s_flow, EX_dstPort, int(dst_port), literal=True, dtype="http://www.w3.org/2001/XMLSchema#integer")
        except Exception:
            pass
    if pd.notna(proto):
        yield triple(s_flow, EX_proto, str(proto).strip(), literal=True)

    # duração (ms)
    if pd.notna(dur):
        try:
            yield triple(s_flow, EX_duration, float(dur), literal=True, dtype="http://www.w3.org/2001/XMLSchema#double")
        except Exception:
            pass

    # label literal
    if pd.notna(label):
        yield triple(s_flow, EX_label, str(label).strip(), literal=True)

# ======= EMISSÃO DE AGREGADOS =======
def emit_aggregates(day_key, agg_map):
    for (bucket, src, dst, proto, dport), st in agg_map.items():
        s_agg = f"{NS_FLOW}{day_key}/agg/{bucket}/{(hash((src,dst,proto,dport)) & 0xffffffff)}"
        s_ip = ip_uri(src)
        d_ip = ip_uri(dst)
        yield triple(s_agg, P_TYPE, EX_AggFlow, literal=False)
        yield triple(s_ip, P_TYPE, EX_IPAddress, literal=False)
        yield triple(d_ip, P_TYPE, EX_IPAddress, literal=False)
        yield triple(s_agg, EX_hasSrc, s_ip, literal=False)
        yield triple(s_agg, EX_hasDst, d_ip, literal=False)
        yield triple(s_agg, EX_bucketStart, int(bucket), literal=True, dtype="http://www.w3.org/2001/XMLSchema#integer")
        yield triple(s_agg, EX_proto, proto, literal=True)
        if dport is not None:
            yield triple(s_agg, EX_dstPort, int(dport), literal=True, dtype="http://www.w3.org/2001/XMLSchema#integer")
        yield triple(s_agg, EX_count, int(st["count"]), literal=True, dtype="http://www.w3.org/2001/XMLSchema#integer")
        yield triple(s_agg, EX_sumBytes, int(st["sumBytes"]), literal=True, dtype="http://www.w3.org/2001/XMLSchema#integer")
        yield triple(s_agg, EX_firstSeen, int(st["firstSeen"]), literal=True, dtype="http://www.w3.org/2001/XMLSchema#integer")
        yield triple(s_agg, EX_lastSeen, int(st["lastSeen"]), literal=True, dtype="http://www.w3.org/2001/XMLSchema#integer")

# ======= PIPELINE POR DIA =======
def process_day(csv_root, fname, args, global_tmax=None, edges_keep=None):
    path = os.path.join(csv_root, fname)
    day_key = day_key_from_name(fname)
    ensure_dir(OUTPUT_DIR)
    out_path = os.path.join(OUTPUT_DIR, f"{day_key}.nt.gz")

    # validação rígida do header
    validate_columns_strict(path, USECOLS)

    # Preparos de pruning
    temporal_td = None
    if args.temporal:
        s = args.temporal.strip().lower()
        if s.endswith("h"):
            hours = float(s[:-1]); temporal_td = timedelta(hours=hours)
        elif s.endswith("s"):
            seconds = float(s[:-1]); temporal_td = timedelta(seconds=seconds)
        else:
            hours = float(s); temporal_td = timedelta(hours=hours)

    labels_keep = None
    if args.labels:
        labels_keep = set(x.strip().lower() for x in args.labels.split(",") if x.strip())

    sample_p = None
    if args.sample is not None:
        sample_p = float(args.sample)
        if not (0.0 < sample_p <= 1.0):
            raise ValueError("--sample precisa estar em (0,1].")

    # Agregação
    do_agg = args.aggregate is not None
    agg_sec = None
    if do_agg:
        s = str(args.aggregate).strip().lower()
        agg_sec = int(float(s[:-1])) if s.endswith("s") else int(float(s))

    print(f"[{day_key}] lendo: {path}")
    n_rows = 0
    n_flows = 0
    n_triples = 0

    agg_map = defaultdict(lambda: {"count":0, "sumBytes":0, "firstSeen":None, "lastSeen":None})

    def flush_aggregates():
        nonlocal n_triples
        gen = emit_aggregates(day_key, agg_map)
        write_triples(out_path, gen)
        n_triples += 10 * len(agg_map)  # aprox. por agregado
        agg_map.clear()

    # cria/zera o arquivo
    with gzip.open(out_path, "wt", encoding="utf-8", newline="") as sink:
        pass

    idx = 0
    for chunk in read_clean_csv_chunks(path, CHUNK):
        # filtra somente as colunas necessárias (todas existem, validado acima)
        chunk = chunk[USECOLS]
        # limpeza mínima
        chunk = chunk.dropna(subset=[COL_SRC_IP, COL_DST_IP, COL_TS, COL_LABEL])

        for row in chunk.itertuples(index=False):
            n_rows += 1
            (flow_id, src_ip, src_port, dst_ip, dst_port, proto, ts_str, dur, fwd_bytes, bwd_bytes, label) = row

            # Top-K backbone (se ativo): manter somente pares (src,dst) em edges_keep
            if edges_keep is not None:
                if (str(src_ip), str(dst_ip)) not in edges_keep:
                    continue

            dt = parse_timestamp(str(ts_str))
            if not dt:
                continue

            if temporal_td is not None and global_tmax is not None:
                if dt < (global_tmax - temporal_td):
                    continue

            if labels_keep and str(label).strip().lower() not in labels_keep:
                continue

            if (sample_p is not None) and (random.random() >= sample_p):
                continue

            if do_agg:
                epoch = int(dt.timestamp())
                bucket = (epoch // agg_sec) * agg_sec
                key = (bucket, str(src_ip), str(dst_ip), str(proto).strip(),
                       int(dst_port) if pd.notna(dst_port) else None)
                st = agg_map[key]
                st["count"] += 1
                fb = int(float(fwd_bytes)) if pd.notna(fwd_bytes) else 0
                bb = int(float(bwd_bytes)) if pd.notna(bwd_bytes) else 0
                st["sumBytes"] += (fb + bb)
                if (st["firstSeen"] is None) or (epoch < st["firstSeen"]):
                    st["firstSeen"] = epoch
                if (st["lastSeen"] is None) or (epoch > st["lastSeen"]):
                    st["lastSeen"] = epoch

                if len(agg_map) >= 200_000:
                    flush_aggregates()
                continue

            # emitir flow individual
            gen = emit_flow_triples(day_key, idx, row,
                                    tmax=global_tmax, temporal_td=temporal_td,
                                    labels_keep=labels_keep, sample_p=sample_p)
            write_triples(out_path, gen)
            n_triples += 12  # estimativa média
            n_flows += 1
            idx += 1

    if do_agg and len(agg_map) > 0:
        flush_aggregates()

    print(f"[{day_key}] linhas lidas: {n_rows:,} | flows emitidos: {n_flows:,} | ~triplas: {n_triples:,} | out: {out_path}")
    return out_path, n_flows, n_triples

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-root", required=True, help="Pasta CSVs/GeneratedLabelledFlows/TrafficLabelling")
    ap.add_argument("--baseline", action="store_true", help="Gera baseline (sem pruning)")
    ap.add_argument("--temporal", help="Janela temporal global, ex: 24h, 48h, 3600s")
    ap.add_argument("--aggregate", help="Agregação por segundos, ex: 60s, 300s")
    ap.add_argument("--topk", type=int, help="Backbone Top-K por src (2 passadas)")
    ap.add_argument("--sample", type=float, help="Amostragem uniforme p (0<p<=1)")
    ap.add_argument("--labels", help="Lista de labels a manter, ex: suspicious,ddos,portscan,benign")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    random.seed(args.seed)

    if not (args.baseline or args.temporal or args.aggregate or args.topk or args.sample or args.labels):
        raise SystemExit("Escolha ao menos uma ação: --baseline | --temporal | --aggregate | --topk | --sample | --labels")

    csv_root = args.csv_root
    if not os.path.isdir(csv_root):
        raise SystemExit(f"Pasta não encontrada: {csv_root}")

    ensure_dir(OUTPUT_DIR)

    # Auxiliares globais
    global_tmax = None
    if args.temporal:
        print("[global] Passo 1: encontrando t_max global...")
        global_tmax = first_pass_tmax(csv_root)
        if not global_tmax:
            raise SystemExit("Não foi possível determinar t_max nos CSVs.")
        print(f"[global] t_max = {global_tmax.isoformat()}")

    edges_keep = None
    if args.topk:
        print("[global] Passo 1 (topk): contando arestas (src,dst)...")
        counts, por_src = pass_count_edges(csv_root)
        print(f"[global] arestas distintas: {len(counts):,}")
        print(f"[global] construindo conjunto Top-{args.topk} por src...")
        edges_keep = build_topk_keep(por_src, args.topk)
        print(f"[global] pares (src,dst) mantidos: {len(edges_keep):,}")

    # Processamento por dia
    total_flows = 0
    total_triples = 0
    outputs = []
    for fname in FILES:
        path = os.path.join(csv_root, fname)
        if not os.path.exists(path):
            print(f"[warn] arquivo ausente: {path}")
            continue
        out, nf, nt = process_day(csv_root, fname, args, global_tmax=global_tmax, edges_keep=edges_keep)
        outputs.append(out)
        total_flows += nf
        total_triples += nt

    print("\n[RESUMO]")
    print(f"Arquivos gerados: {len(outputs)} (pasta '{OUTPUT_DIR}')")
    for o in outputs:
        print(f" - {o}")
    print(f"~Flows emitidos (total): {total_flows:,}")
    print(f"~Triplas (estimadas):    {total_triples:,}")
    print("\nPronto para carregar no Fuseki com named graphs (um por dia).")

if __name__ == "__main__":
    main()
