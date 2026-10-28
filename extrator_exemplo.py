"""
Extração incremental de dados via API REST -> PostgreSQL

Versão ANONIMIZADA de um script real de ETL. O nome do sistema, os endpoints,
as credenciais e os dados foram substituídos por exemplos genéricos.
A lógica (paginação, tratamento e carga incremental) é a mesma do original.

Fluxo:
  1. Abre uma sessão HTTP com retry automático
  2. Busca a página 1 e descobre quantas páginas existem
  3. Percorre as demais páginas
  4. Trata os dados com pandas
  5. Calcula um hash por registro
  6. Compara com o que já está no banco (novos x alterados x iguais)
  7. Grava apenas o necessário, dentro de uma transação
"""
import hashlib
import json
import os
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import bindparam, create_engine, inspect, text
from sqlalchemy.types import JSON, String
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# CONFIGURAÇÃO (tudo vem de variáveis de ambiente, nada fica no código)
# ---------------------------------------------------------------------------
API_URL = os.environ["API_URL"]            # ex.: https://api.exemplo-erp.com/v1/compradores/
API_TOKEN = os.environ["API_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]  # ex.: postgresql://usuario:senha@host:5432/banco

SCHEMA_DESTINO = "vendas_erp"
TABELA_DESTINO = "compradores"
CAMPO_CHAVE = "codigo"        # identificador único do registro na origem
HASH_COL = "hash_registro"    # coluna que guarda o "resumo" de cada linha
REGISTROS_POR_PAGINA = 50
PAUSA_ENTRE_PAGINAS = 0.4     # segundos, para respeitar o limite da API

# Colunas cujo nome contém algum destes trechos são convertidas para data
PADROES_DATA = ["data_", "_data", "_date", "_dalt", "_dinc", "_previsao"]

engine = create_engine(DATABASE_URL)


# ---------------------------------------------------------------------------
# 1. SESSÃO HTTP COM RETRY
# ---------------------------------------------------------------------------
def criar_sessao():
    sessao = requests.Session()
    sessao.headers.update({
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    # Retry automático para erros temporários do servidor (429, 5xx),
    # com espera crescente entre as tentativas (backoff)
    estrategia = Retry(
        total=5,
        backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    sessao.mount("https://", HTTPAdapter(max_retries=estrategia))
    return sessao


# ---------------------------------------------------------------------------
# 2. PAGINAÇÃO
# ---------------------------------------------------------------------------
def buscar_pagina(sessao, pagina, tentativas=5):
    """Busca UMA página. Tenta de novo em caso de timeout ou queda de conexão."""
    payload = {"pagina": pagina, "registros_por_pagina": REGISTROS_POR_PAGINA}
    for i in range(tentativas):
        try:
            resposta = sessao.post(API_URL, json=payload, timeout=30)
            resposta.raise_for_status()
            return resposta.json()
        except Exception as erro:
            print(f"Erro na página {pagina} (tentativa {i + 1}/{tentativas}): {erro}")
            if i < tentativas - 1:
                time.sleep((i + 1) * 3)   # espera 3s, 6s, 9s...
            else:
                raise


def extrair_todas_paginas(sessao):
    """
    A primeira chamada devolve os registros da página 1 e também o total de
    páginas. Com esse número, o laço percorre as páginas restantes.
    """
    primeira = buscar_pagina(sessao, 1)
    if "erro" in primeira:
        raise RuntimeError(f"Erro retornado pela API: {primeira['erro']}")

    total_paginas = int(primeira.get("total_paginas", 1))
    registros = list(primeira.get("dados", []))
    print(f"Total de páginas: {total_paginas} | Total de registros: {primeira.get('total_registros', 0)}")

    paginas_com_falha = []
    for pagina in range(2, total_paginas + 1):
        try:
            print(f"Buscando página {pagina}/{total_paginas}")
            registros.extend(buscar_pagina(sessao, pagina).get("dados", []))
        except Exception:
            # Uma página com falha não derruba a extração inteira
            paginas_com_falha.append(pagina)
        time.sleep(PAUSA_ENTRE_PAGINAS)

    return registros, paginas_com_falha


# ---------------------------------------------------------------------------
# 3. HASH DO REGISTRO (para detectar alterações)
# ---------------------------------------------------------------------------
def _normalizar_valor_hash(valor):
    """Converte qualquer valor para uma forma estável antes de gerar o hash."""
    if isinstance(valor, dict):
        return {str(k): _normalizar_valor_hash(v)
                for k, v in sorted(valor.items(), key=lambda item: str(item[0]))}
    if isinstance(valor, (list, tuple)):
        return [_normalizar_valor_hash(item) for item in valor]
    if isinstance(valor, pd.Timestamp):
        return None if pd.isna(valor) else valor.isoformat()
    if hasattr(valor, "item") and not isinstance(valor, (str, bytes)):
        try:
            valor = valor.item()        # numpy -> tipo nativo do Python
        except Exception:
            pass
    try:
        if pd.isna(valor):
            return None                 # NaN, NaT e None viram sempre None
    except (TypeError, ValueError):
        pass
    return valor


def calcular_hash_linha(linha):
    """MD5 do conteúdo da linha (exceto a própria coluna de hash)."""
    conteudo = {c: _normalizar_valor_hash(v) for c, v in linha.items() if c != HASH_COL}
    texto = json.dumps(conteudo, sort_keys=True, ensure_ascii=False,
                       separators=(",", ":"), default=str)
    return hashlib.md5(texto.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 4. TRATAMENTO DOS DADOS
# ---------------------------------------------------------------------------
def tratar_dados(registros):
    # Achata o JSON aninhado: {"endereco": {"cidade": "X"}} -> endereco_cidade
    df = pd.json_normalize(registros)
    df.columns = [c.replace(".", "_").lower() for c in df.columns]

    # Converte colunas de data (formato dd/mm/aaaa); valores inválidos viram NaT
    for coluna in df.columns:
        if any(padrao in coluna for padrao in PADROES_DATA):
            df[coluna] = pd.to_datetime(df[coluna], format="%d/%m/%Y", errors="coerce")

    if CAMPO_CHAVE not in df.columns:
        raise KeyError(f"Campo-chave '{CAMPO_CHAVE}' não encontrado nos dados.")

    # A chave vira texto e duplicatas são removidas (fica a última ocorrência)
    df[CAMPO_CHAVE] = df[CAMPO_CHAVE].astype(str)
    df = df.drop_duplicates(subset=[CAMPO_CHAVE], keep="last").reset_index(drop=True)

    df[HASH_COL] = df.apply(calcular_hash_linha, axis=1)
    return df


# ---------------------------------------------------------------------------
# 5. CARGA INCREMENTAL
# ---------------------------------------------------------------------------
def ler_existentes():
    """Lê só a chave e o hash do que já está no banco (leve e rápido)."""
    if not inspect(engine).has_table(TABELA_DESTINO, schema=SCHEMA_DESTINO):
        return pd.DataFrame(columns=[CAMPO_CHAVE, HASH_COL])
    consulta = text(f'SELECT "{CAMPO_CHAVE}", "{HASH_COL}" '
                    f'FROM "{SCHEMA_DESTINO}"."{TABELA_DESTINO}"')
    with engine.connect() as conn:
        existentes = pd.read_sql(consulta, conn)
    existentes[CAMPO_CHAVE] = existentes[CAMPO_CHAVE].astype(str)
    return existentes


def separar_novos_e_alterados(df, existentes):
    if existentes.empty:
        return df.copy(), df.iloc[0:0].copy()

    hashes_banco = dict(zip(existentes[CAMPO_CHAVE], existentes[HASH_COL]))
    existe = df[CAMPO_CHAVE].isin(hashes_banco.keys())
    hash_antigo = df[CAMPO_CHAVE].map(hashes_banco)

    novos = df[~existe].copy()                               # chave que não existe no banco
    alterados = df[existe & (hash_antigo != df[HASH_COL])]   # mesma chave, hash diferente
    return novos, alterados.copy()


def gravar(novos, alterados):
    para_inserir = pd.concat([novos, alterados], ignore_index=True)
    if para_inserir.empty:
        print("Nenhum registro novo ou alterado.")
        return

    # Colunas com dict/list são gravadas como JSON; o hash tem 32 caracteres
    tipos = {}
    for coluna in para_inserir.columns:
        amostra = para_inserir[coluna].dropna()
        if not amostra.empty and isinstance(amostra.iloc[0], (dict, list)):
            tipos[coluna] = JSON
    tipos[HASH_COL] = String(32)

    # engine.begin() abre uma TRANSAÇÃO: ou tudo é gravado, ou nada é
    with engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_DESTINO}"')

        if not alterados.empty:
            # Os registros alterados são apagados e reinseridos já atualizados
            apagar = text(
                f'DELETE FROM "{SCHEMA_DESTINO}"."{TABELA_DESTINO}" '
                f'WHERE "{CAMPO_CHAVE}" IN :chaves'
            ).bindparams(bindparam("chaves", expanding=True))
            conn.execute(apagar, {"chaves": alterados[CAMPO_CHAVE].tolist()})

        para_inserir.to_sql(TABELA_DESTINO, conn, schema=SCHEMA_DESTINO,
                            if_exists="append", index=False, dtype=tipos)

    print(f"OK: {len(novos)} inseridos e {len(alterados)} atualizados.")


# ---------------------------------------------------------------------------
# EXECUÇÃO
# ---------------------------------------------------------------------------
def executar():
    print(f"Iniciando extração em {pd.Timestamp.now()}")
    sessao = criar_sessao()

    registros, paginas_com_falha = extrair_todas_paginas(sessao)
    if paginas_com_falha:
        print(f"ATENÇÃO: páginas com falha (serão pegas na próxima execução): {paginas_com_falha}")
    if not registros:
        print("Nenhum registro retornado.")
        return

    df = tratar_dados(registros)
    existentes = ler_existentes()
    novos, alterados = separar_novos_e_alterados(df, existentes)
    print(f"No banco: {len(existentes)} | Novos: {len(novos)} | Alterados: {len(alterados)}")
    gravar(novos, alterados)


if __name__ == "__main__":
    executar()
