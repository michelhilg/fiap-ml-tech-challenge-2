# Tech Challenge - Fase 2 - Pós MLET - FIAP

Este script Python extrai dados horários de todas as ações que compõem o índice IBOVESPA, os processa e envia para um bucket na AWS S3 em formato Parquet, com particionamento diário.

Este projeto atende aos requisitos de coleta e ingestão de dados na camada *raw* de um Data Lake, conforme o **Tech Challenge Fase 2 - Pós MLET FIAP**.

## Funcionalidades Principais

-   **Extração Dinâmica**: Busca a lista de tickers do IBOVESPA diretamente da API da B3.
-   **Coleta de Dados**: Baixa o histórico de cotações (OHLCV) usando `yfinance`.
-   **Ingestão no S3**: Salva os dados no formato Parquet, particionando por data no padrão `raw/data=YYYY-MM-DD/`.
-   **Sinal de Conclusão**: Cria um arquivo `_SUCCESS` para acionar pipelines de dados subsequentes.

## Pré-requisitos

1.  **Python 3.8+**
2.  **Credenciais AWS** configuradas no ambiente (ex: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
3.  Uso do arquivo `requirements.txt` para as dependências.

## Como Executar

### 1. Criar e ativar ambiente virtual

```bash
# Criar o ambiente virtual
python -m venv venv

# Ativar no Linux/macOS
source venv/bin/activate

# Ativar no Windows (PowerShell)
.\venv\Scripts\Activate.ps1
```

---

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

---

### 3. Executar o script

```bash
python scripts/b3_data.py
```

---

