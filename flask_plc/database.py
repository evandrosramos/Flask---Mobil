import os
import pgserver
import psycopg2
import psycopg2.extensions
from sqlalchemy import (
    create_engine, Column, BigInteger, Integer,
    DateTime, Boolean, Float, String, Text, text
)
from sqlalchemy.orm import DeclarativeBase, Session
from datetime import datetime

# ─── Inicialização do Servidor PostgreSQL Embutido ─────────────────────────
PGDATA_DIR = os.path.join(os.path.dirname(__file__), 'pgdata')

print('🔄 Iniciando servidor PostgreSQL embutido (pgserver)...')
_pg = pgserver.get_server(PGDATA_DIR, cleanup_mode='stop')
print('✅ PostgreSQL iniciado!')

# Conecta no banco padrão para criar o banco específico do TL14
_uri_postgres = _pg.get_uri(database='postgres')
_conn = psycopg2.connect(_uri_postgres)
_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
_cur = _conn.cursor()

_cur.execute("SELECT 1 FROM pg_database WHERE datname = 'tl14_monitor'")
if not _cur.fetchone():
    _cur.execute('CREATE DATABASE tl14_monitor')
    print('✅ Banco tl14_monitor criado.')
else:
    print('✅ Banco tl14_monitor já existe.')
    
_cur.close()
_conn.close()

# Conecta no banco da nossa aplicação
DB_URL = _pg.get_uri(database='tl14_monitor')
engine = create_engine(DB_URL, echo=False)


# ─── Modelo Base ──────────────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass

# ══════════════════════════════════════════════════════════════════════════
# TABELA 1 — STATUS GERAL DA MÁQUINA (TL14)
# ══════════════════════════════════════════════════════════════════════════
class StatusMaquina(Base):
    __tablename__ = "status_maquina"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now, index=True)
    
    modo_producao = Column(Boolean)
    modo_manutencao = Column(Boolean)
    sem_defeitos = Column(Boolean)
    missao_em_andamento = Column(Boolean)
    posicao_translacao = Column(Float)
    posicao_elevacao = Column(Float)

# ══════════════════════════════════════════════════════════════════════════
# TABELA 2 — HISTÓRICO DE MISSÕES (TL14)
# ══════════════════════════════════════════════════════════════════════════
class HistoricoMissoes(Base):
    __tablename__ = "historico_missoes"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp_inicio = Column(DateTime, nullable=False, index=True)
    timestamp_fim = Column(DateTime, nullable=True)
    duracao_segundos = Column(Float, nullable=True)
    
    tipo_missao = Column(String(50)) # Carregamento / Descarregamento / Transferência
    origem = Column(String(50))
    destino = Column(String(50))
    status_fim = Column(String(50))  # OK / FALHA
    
    velocidade_media_translacao = Column(Float, nullable=True)
    velocidade_media_elevacao = Column(Float, nullable=True)
    ciclo_completo = Column(Boolean, default=False)

# ══════════════════════════════════════════════════════════════════════════
# TABELA 3 — CONTADORES GERAIS DE PRODUÇÃO
# ══════════════════════════════════════════════════════════════════════════
class ContadoresProducao(Base):
    __tablename__ = "contadores_producao"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now, index=True)
    
    missoes_total = Column(Integer, default=0)
    missoes_entrada = Column(Integer, default=0)
    missoes_saida = Column(Integer, default=0)
    missoes_falha = Column(Integer, default=0)

# ══════════════════════════════════════════════════════════════════════════
# TABELA 4 — REGISTRO DE EVENTOS (ALARMES DO TL14)
# ══════════════════════════════════════════════════════════════════════════
class AlarmesEvento(Base):
    __tablename__ = "alarmes_evento"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now, index=True)
    tipo = Column(String(20)) # ATIVADO / DESATIVADO
    numero = Column(Integer)
    descricao = Column(Text)
    palavra = Column(Integer)
    bit = Column(Integer)

# ══════════════════════════════════════════════════════════════════════════
# TABELA 5 — CONTROLE DE ESTADO DE ALARMES (DETECÇÃO DE BORDA)
# ══════════════════════════════════════════════════════════════════════════
class AlarmesEstadoAnterior(Base):
    """ Tabela auxiliar para detector de borda do coletor """
    __tablename__ = "alarmes_estado_anterior"
    
    palavra = Column(Integer, primary_key=True)
    valor = Column(BigInteger, nullable=False, default=0)
    atualizado = Column(DateTime, nullable=False, default=datetime.now)

# ══════════════════════════════════════════════════════════════════════════
# FUNÇÃO DE INICIALIZAÇÃO
# ══════════════════════════════════════════════════════════════════════════
def criar_tabelas():
    Base.metadata.create_all(engine)
    
    # Inicializa os slots de DINTs de falhas para o trigger de borda (FaultsB18[0] até [9])
    with engine.begin() as conn:
        for i in range(10): 
            conn.execute(text("""
                INSERT INTO alarmes_estado_anterior (palavra, valor, atualizado) 
                VALUES (:p, 0, NOW()) 
                ON CONFLICT (palavra) DO NOTHING
            """), {"p": i})
            
    print("✅ Tabelas SQL verificadas/criadas com sucesso.")
