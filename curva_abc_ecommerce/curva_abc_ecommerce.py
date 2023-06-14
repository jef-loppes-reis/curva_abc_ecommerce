import pandas as pd
from pecista import Postgres
from tqdm import tqdm
from math import floor
import warnings
warnings.filterwarnings('ignore')


def read_postgres(query:str):
    """Funcao para importar tabelas do Postgres.

    Args:
        query (str): Select para o DB.

    Returns:
        _type_: Retorna a tabela em DataSet.
    """    
    with Postgres() as db:
        df = db.query(query)
    return df

def creating_col_fat_abs(df_curvaABC:pd.DataFrame):
    """Cria uma coluna, calculando o total de faturamento de vendas.

    Args:
        df_curvaABC (pd.DataFrame): _description_

    Returns:
        _type_: _description_
    """    
    for idx in tqdm(df_curvaABC.index):
        codpro = df_curvaABC.loc[idx, 'cod_pro']
        df_curvaABC.loc[idx,'soma_qtd_vend'] = df_merge[ df_merge['cod_pro'] == codpro].agg({'qtde_ven':sum})[0]
        df_curvaABC.loc[idx, 'faturamento_absoluto'] = df_curvaABC.loc[idx,'soma_qtd_vend'].astype(int) * min(df_merge[ df_merge['cod_pro'] == codpro ]['preco'].astype(float))

    return df_curvaABC

def curve_classification(df_curvaABC:pd.DataFrame):
    """Classificacoes dos produtos em curva de vendas.

    Args:
        df_curvaABC (pd.DataFrame): _description_

    Returns:
        _type_: _description_
    """    
    for idx in df_curvaABC.index:
        if (df_curvaABC.loc[idx, 'percente_acomulado'] >= 0) & (df_curvaABC.loc[idx, 'percente_acomulado'] <= 50):
            df_curvaABC.loc[idx, 'classificacao'] = 'A'

        elif (df_curvaABC.loc[idx, 'percente_acomulado'] > 50) & (df_curvaABC.loc[idx, 'percente_acomulado'] <= 75):
            df_curvaABC.loc[idx, 'classificacao'] = 'B'

        elif (df_curvaABC.loc[idx, 'percente_acomulado'] > 75) & (df_curvaABC.loc[idx, 'percente_acomulado'] <= 90):
            df_curvaABC.loc[idx, 'classificacao'] = 'C'
        
        elif (df_curvaABC.loc[idx, 'percente_acomulado'] > 90) & (df_curvaABC.loc[idx, 'percente_acomulado'] <= 95):
            df_curvaABC.loc[idx, 'classificacao'] = 'D'

        elif (df_curvaABC.loc[idx, 'percente_acomulado'] > 95) & (df_curvaABC.loc[idx, 'percente_acomulado'] <= 98):
            df_curvaABC.loc[idx, 'classificacao'] = 'E'

        else:
            df_curvaABC.loc[idx, 'classificacao'] = 'F'

    return df_curvaABC

if __name__ == '__main__':

    # Leitura table (produtos e prd_loja)
    df_produtos = read_postgres('SELECT codpro, num_fab, produto FROM "D-1".produto ')
    df_estoque_produto = read_postgres('SELECT cd_loja, codpro, estoque FROM "H-1".prd_loja ')
    df_estoque_grupo = df_estoque_produto.groupby('codpro').agg({'estoque':sum}).reset_index()
    # Dataframe estoque somente estoque PECISTA
    df_estoque_pecista = df_estoque_produto.query("cd_loja == '01' ")[['codpro', 'estoque']]
    # Merge tb_rodutos com tb_estoque_grupo
    df_produtos = pd.merge(df_produtos,df_estoque_grupo,on='codpro',how='inner'
    )
    # Merge tb_merge_produto com tb_estoque_precista
    df_produtos = pd.merge(df_produtos,df_estoque_pecista,on='codpro',how='inner').rename({'estoque_x':'estoque', 'estoque_y':'estoque_pecista'}, axis=1).fillna({'estoque_pecista':0})
    # Leitura tabela (pedido)
    df_pedidos = read_postgres(
        f'SELECT cd_loja, nu_nota, dt_emissao, codcli, codvde, observa, numnota, indtrans FROM "D-1".pedido WHERE forma_pgto = \'M\''
    )
    # Leitura tabela (prod_ped)
    df_prod_pedidos = read_postgres(
        """SELECT cd_loja, nu_nota, nu_item, cod_pro, qtde_ven, preco, dt_emissao, codcli, codvde, tipo
        FROM "H-1".vw_prod_ped
        WHERE codcli in ('99999','88888') and dt_emissao > current_date - 90 """
    )
    # Merge tabela (pedidos) com tabela (prod_ped)
    df_merge = pd.merge(df_pedidos,df_prod_pedidos,on='nu_nota',how='inner')
    # Criando Dataframe CurvaABC
    df_curvaABC = df_merge.drop_duplicates(subset='cod_pro')
    # Criando coluna de Faturamente Absoluto
    df_curvaABC = creating_col_fat_abs(df_curvaABC)
    df_curvaABC = df_curvaABC.sort_values(by='faturamento_absoluto', ascending=False).reset_index(drop=True)
    soma_faturamento_absoluto = round(df_curvaABC.faturamento_absoluto.sum(), 2)
    # Criando coluna de faturamento Relativo
    for idx in tqdm(df_curvaABC.index): df_curvaABC.loc[idx, 'faturamento_relativo_%'] = (df_curvaABC.loc[idx, 'faturamento_absoluto'] / soma_faturamento_absoluto) * 100
    # Criando coluna de Valor Acomulado
    valor_acomulado = 0
    for idx in tqdm(df_curvaABC.index):
        df_curvaABC.loc[idx, 'percente_acomulado'] = valor_acomulado + df_curvaABC.loc[idx, 'faturamento_relativo_%']
        valor_acomulado += df_curvaABC.loc[idx, 'faturamento_relativo_%']
    # Criando coluna de classificacao das curvas
    df_curvaABC = curve_classification(df_curvaABC)
    #
    df_curvaABC = df_curvaABC[['cd_loja_x','cod_pro','soma_qtd_vend','faturamento_absoluto','faturamento_relativo_%','percente_acomulado','classificacao']].rename({'cod_pro':'codpro'}, axis=1)
    #
    df_curvaABC = pd.merge(df_produtos,df_curvaABC,on='codpro',how='inner')
    #
    df_curvaABC['media_vendas_30'] = df_curvaABC.soma_qtd_vend / 3
    #
    df_curvaABC = pd.merge(df_produtos[['codpro', 'produto']],df_curvaABC,on='codpro',how='left').query(" ~num_fab.isna() ").reset_index(drop=True)
    # Salvando Excel
    df_curvaABC.drop(['produto_y','cd_loja_x'], axis=1).sort_values('percente_acomulado').to_excel('df_curvaABC.xlsx', index=False)
    print(input('\nFinalizado !\nTecla enter para sair...'))