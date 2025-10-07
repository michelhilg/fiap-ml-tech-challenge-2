import yfinance as yf
import pandas as pd
import requests
import json
import base64
import time


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
    

def baixar_e_tratar_dados_b3(tickers, periodo='5d', intervalo='1d'):
    """
    Baixa dados para uma lista de tickers, corrige o formato anômalo retornado
    pela biblioteca yfinance e consolida os resultados em um único DataFrame.
    """
    print(f"\nIniciando download para {len(tickers)} tickers...")
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
                dados_ticker = dados_ticker.stack(level=1).reset_index()
                dados_ticker.rename(columns={'level_1': 'Ticker', 'Date': 'data'}, inplace=True)
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
    df_final.rename(columns={'index': 'data'}, inplace=True)
    
    return df_final

if __name__ == "__main__":
    
    lista_de_tickers = obter_tickers_ibov()
    resultado_final = baixar_e_tratar_dados_b3(lista_de_tickers, periodo='5d')

    if resultado_final is not None:
        colunas_ordenadas = ['data', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
        resultado_final = resultado_final[colunas_ordenadas]

        print("\n\n--- RESULTADO FINAL CONSOLIDADO ---")
        print(resultado_final)
        
        print("\n--- Informações do DataFrame ---")
        resultado_final.info()

        # Salvar o resultado final em um arquivo CSV
        try:
            nome_arquivo = "dados_b3_consolidados.csv"
            resultado_final.to_csv(nome_arquivo, index=False, encoding='utf-8')
            print(f"\n[SUCESSO] Dados salvos em '{nome_arquivo}'")
        except Exception as e:
            print(f"\n[ERRO] Falha ao salvar arquivo CSV: {e}")