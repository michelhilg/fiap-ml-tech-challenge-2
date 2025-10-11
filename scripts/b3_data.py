import yfinance as yf
import pandas as pd
import requests
import json
import base64
import boto3
from io import BytesIO 

def obter_tickers_ibov():
    """
    Busca a lista de tickers que compõem o índice IBOVESPA diretamente
    de uma API da B3. Esta é uma fonte oficial e mais confiável.
    """
    print("Obtendo a lista de tickers do IBOVESPA da B3...")
    try:
        # O payload é um JSON codificado em base64 na URL da B3
        payload = {
            "language": "pt-br",
            "pageNumber": 1,
            "pageSize": 120, 
            "index": "IBOV",
            "segment": "LTC"
        }

        payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
        url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{payload_b64}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()  
        data = response.json()
        tickers = [f"{result['cod']}.SA" for result in data['results']]
        
        print(f"Encontrados {len(tickers)} tickers no IBOVESPA.")
        return tickers
        
    except Exception as e:
        print(f"Erro ao obter a lista de tickers da B3: {e}")
        print("Usando uma lista de fallback para continuar.")
        return ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'MGLU3.SA', 'WEGE3.SA']
    

def baixar_e_tratar_dados_b3(tickers, periodo='2d', intervalo='1h'):
    """
    Baixa dados para uma lista de tickers, corrige o formato anômalo retornado
    pela biblioteca yfinance e consolida os resultados em um único DataFrame.
    """
    print(f"\nIniciando download para {len(tickers)} tickers (Intervalo: {intervalo})...")
    lista_dataframes_corrigidos = []

    for ticker_atual in tickers:
        print(f"Processando: {ticker_atual}")
        try:
            dados_ticker = yf.download(
                tickers=ticker_atual,
                period=periodo,
                interval=intervalo,
                progress=False,
                auto_adjust=True
            )

            if dados_ticker.empty:
                continue

            if isinstance(dados_ticker.columns, pd.MultiIndex):
                dados_ticker = dados_ticker.stack(level=1, future_stack=True).reset_index()
                
                # Identifica a coluna de data/hora (pode ser 'Date' ou 'Datetime')
                date_col_name = 'Datetime' if 'Datetime' in dados_ticker.columns else 'Date'
                dados_ticker.rename(columns={'level_1': 'Ticker', date_col_name: 'data'}, inplace=True)
                
                dados_ticker = dados_ticker[['data', 'Open', 'High', 'Low', 'Close', 'Volume']]
                dados_ticker.set_index('data', inplace=True)

            dados_ticker['ticker'] = ticker_atual.replace('.SA', '')
            lista_dataframes_corrigidos.append(dados_ticker)

        except Exception as e:
            print(f"ERRO ao processar {ticker_atual}: {e}")

    if not lista_dataframes_corrigidos:
        print("Nenhum dado foi baixado com sucesso.")
        return None

    df_final = pd.concat(lista_dataframes_corrigidos).reset_index()
    
    # Padroniza a coluna de data/hora que vem do índice
    if 'index' in df_final.columns:
        df_final.rename(columns={'index': 'data'}, inplace=True)
    
    return df_final


def enviar_para_s3_particionado(df, bucket_name):
    """
    Recebe um DataFrame, particiona por dia e envia cada partição
    em formato Parquet para a pasta 'raw' do S3.
    """
    if df is None or df.empty:
        print("DataFrame vazio. Nenhum dado para enviar ao S3.")
        return

    s3_client = boto3.client('s3')
    
    df['data'] = pd.to_datetime(df['data'])
    
    datas_unicas = df['data'].dt.date.unique()
    print(f"\nEncontradas {len(datas_unicas)} datas únicas para upload no S3.")

    for data_particao in datas_unicas:
        data_str = data_particao.strftime('%Y-%m-%d')
        print(f"Processando partição S3: data={data_str}")
        
        df_particionado = df[df['data'].dt.date == data_particao]
        
        s3_path = f"raw/data={data_str}/dados_b3_horario.parquet"
        
        try:
            buffer = BytesIO()
            
            df_particionado.to_parquet(
                buffer, 
                index=False, 
                engine='pyarrow', 
                use_deprecated_int96_timestamps=True
            )           
            
            buffer.seek(0)
            
            s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=buffer)
            print(f"  -> [SUCESSO] Partição enviada para s3://{bucket_name}/{s3_path}")
        except Exception as e:
            print(f"  -> [ERRO] Falha ao enviar a partição para o S3: {e}")


def sinalizar_conclusao_upload(bucket_name, data_hoje):
    """
    Cria e envia um arquivo vazio _SUCCESS para o S3 para sinalizar
    o fim do carregamento e acionar o pipeline.
    """
    s3_client = boto3.client('s3')
    
    data_str = data_hoje.strftime('%Y-%m-%d')
    s3_path = f"raw/data={data_str}/_SUCCESS"
    
    try:
        s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=b'')
        print(f"\n[SUCESSO] Arquivo de sinalização enviado para s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"\n[ERRO] Falha ao enviar arquivo de sinalização: {e}")
            

if __name__ == "__main__":
    
    lista_de_tickers = obter_tickers_ibov()
    resultado_final = baixar_e_tratar_dados_b3(lista_de_tickers, periodo='5d', intervalo='1h')

    if resultado_final is not None:
        resultado_final['data'] = pd.to_datetime(resultado_final['data'])

        print("\nConvertendo fuso horário para Brasília (America/Sao_Paulo)...")
        resultado_final['data'] = resultado_final['data'].dt.tz_convert('America/Sao_Paulo')
        
        colunas_ordenadas = ['data', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
        resultado_final = resultado_final[colunas_ordenadas]

        print("\n--- DataFrame final pronto (com horário de Brasília) ---")
        print(resultado_final.head(500))

        bucket = 'fiap-fase2-mlet-6'
        enviar_para_s3_particionado(resultado_final, bucket)

        # Marcação de sucesso
        if not resultado_final.empty:
            data_mais_recente = resultado_final['data'].max().date()
            sinalizar_conclusao_upload(bucket, data_mais_recente)