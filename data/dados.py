# dados.py
import pandas as pd

def carregar_dados():
    df_load = pd.read_csv("data/demografia_mundial.csv", sep=";", encoding="utf-8", 
                     skipinitialspace=True, decimal=",", low_memory=False)
    
    # CORREÇÃO: Adicionar .copy() para evitar o SettingWithCopyWarning
    df = df_load[df_load["Type"] == "Region"].copy()
    
    # Limpar colunas
    df.columns = [col.strip() for col in df.columns]
    df.rename(columns={"Region, subregion, country or area *": "Regiao"}, inplace=True)
    df["Regiao"] = df["Regiao"].str.strip()
    

    # Filtrar regiões relevantes
    regioes_validas = [
        "Africa", "Asia", "Europe",
        "Latin America and the Caribbean", "Northern America", "Oceania"
    ]
    df = df[df["Regiao"].isin(regioes_validas)]

    # Mapear para continentes
    def mapear_continente(regiao):
        if regiao in ["Latin America and the Caribbean", "Northern America"]:
            return "América"
        elif regiao == "Africa":
            return "África"
        elif regiao == "Asia":
            return "Ásia"
        elif regiao == "Europe":
            return "Europa"
        elif regiao == "Oceania":
            return "Oceania"
        return None

    df["Continente"] = df["Regiao"].apply(mapear_continente)

    # Colunas de interesse
    pop_col = "TotalPopulation,asof1July(thousands)"
    densidade = "Population Density, as of 1 July (persons per square km)"
    racio = "Population Sex Ratio, as of 1 July (males per 100 females)"
    crescimento = "PopulationGrowthRate(percentage)"
    idade_media="Median Age, as of 1 July (years)"
    taxa_alteracao_natural="NaturalChange,BirthsminusDeaths(thousands)"
    nascimentos="Births(thousands)"
    obitos="TotalDeaths(thousands)"
    esperanca_vida="LifeExpectancyatBirth,bothsexes(years)"
    esperanca_vida_homens80="MaleLifeExpectancyatAge80(years)"
    esperanca_vida_mulheres80="FemaleLifeExpectancyatAge80(years)"
    mortalidade_antes40="MortalitybeforeAge40,bothsexes(deathsunderage40per1,000livebirths)"
    mortalidade_antes60="MortalitybeforeAge60,bothsexes(deathsunderage60per1,000livebirths)"
    mortalidade_entre15e50="MortalitybetweenAge15and50,bothsexes(deathsunderage50per1,000aliveatage15)"
    taxa_migracao_liquida="NetNumberofMigrants(thousands)"
    mortalidade_entre15e50Homens="MaleMortalitybetweenAge15and50(deathsunderage50per1,000malesaliveatage15)"
    mortalidade_entre15e50Mulheres="FemaleMortalitybetweenAge15and50(deathsunderage50per1,000femalesaliveatage15)"

    # Converter tipos
    df[pop_col] = pd.to_numeric(df[pop_col], errors="coerce")
    df[pop_col] = df[pop_col] / 1e3  # Converter para milhões
    df[densidade] = pd.to_numeric(df[densidade].astype(str).str.replace(",", "."), errors="coerce")
    df[racio] = pd.to_numeric(df[racio].astype(str).str.replace(",", "."), errors="coerce")
    df[racio] = df[racio] / 100
    df[crescimento] = pd.to_numeric(df[crescimento].astype(str).str.replace(",", "."), errors="coerce")
    df[idade_media] = pd.to_numeric(df[idade_media], errors="coerce")
    df[taxa_alteracao_natural] = pd.to_numeric(df[taxa_alteracao_natural].astype(str).str.replace(",", "."), errors="coerce")
    df[nascimentos] = pd.to_numeric(df[nascimentos].astype(str).str.replace(",", "."), errors="coerce")
    df[obitos] = pd.to_numeric(df[obitos].astype(str).str.replace(",", "."), errors="coerce")
    df[esperanca_vida] = pd.to_numeric(df[esperanca_vida].astype(str).str.replace(",", "."), errors="coerce")
    df[esperanca_vida_homens80] = pd.to_numeric(df[esperanca_vida_homens80].astype(str).str.replace(",", "."), errors="coerce")
    df[esperanca_vida_mulheres80] = pd.to_numeric(df[esperanca_vida_mulheres80].astype(str).str.replace(",", "."), errors="coerce")
    df[mortalidade_antes40] = pd.to_numeric(df[mortalidade_antes40].astype(str).str.replace(",", "."), errors="coerce")
    df[mortalidade_antes60] = pd.to_numeric(df[mortalidade_antes60].astype(str).str.replace(",", "."), errors="coerce")
    df[mortalidade_entre15e50] = pd.to_numeric(df[mortalidade_entre15e50].astype(str).str.replace(",", "."), errors="coerce")
    df[taxa_migracao_liquida] = pd.to_numeric(df[taxa_migracao_liquida].astype(str).str.replace(",", "."), errors="coerce") 
    df[mortalidade_entre15e50Homens] = pd.to_numeric(df[mortalidade_entre15e50Homens].astype(str).str.replace(",", "."), errors="coerce")
    df[mortalidade_entre15e50Mulheres] = pd.to_numeric(df[mortalidade_entre15e50Mulheres].astype(str).str.replace(",", "."), errors="coerce")
    
    # Agregações
    df_pop = df.groupby(["Continente", "Year"], observed=False)[pop_col].sum().reset_index().rename(columns={pop_col: "Populacao"})
    df_dens = df.groupby(["Continente", "Year"], observed=False)[densidade].mean().reset_index().rename(columns={densidade: "Densidade"})
    df_racio = df.groupby(["Continente", "Year"], observed=False)[racio].mean().reset_index().rename(columns={racio: "RacioGenero"})
    df_cresc = df.groupby(["Continente", "Year"], observed=False)[crescimento].mean().reset_index().rename(columns={crescimento: "Crescimento"})
    df_idade_media = df.groupby(["Continente", "Year"], observed=False)[idade_media].mean().reset_index().rename(columns={idade_media: "IdadeMedia"})
    df_taxa_alteracao_natural = df.groupby(["Continente", "Year"], observed=False)[taxa_alteracao_natural].mean().reset_index().rename(columns={taxa_alteracao_natural: "TaxaAlteracaoNatural"})
    df_nascimentos = df.groupby(["Continente", "Year"], observed=False)[nascimentos].sum().reset_index().rename(columns={nascimentos: "Nascimentos"})
    df_obitos = df.groupby(["Continente", "Year"], observed=False)[obitos].sum().reset_index().rename(columns={obitos: "Obitos"})
    df_esperanca_vida = df.groupby(["Continente", "Year"], observed=False)[esperanca_vida].mean().reset_index().rename(columns={esperanca_vida: "EsperancaVida"})
    df_esperanca_vida_homens80 = df.groupby(["Continente", "Year"], observed=False)[esperanca_vida_homens80].mean().reset_index().rename(columns={esperanca_vida_homens80: "EsperancaVidaHomens80"})
    df_esperanca_vida_mulheres80 = df.groupby(["Continente", "Year"], observed=False)[esperanca_vida_mulheres80].mean().reset_index().rename(columns={esperanca_vida_mulheres80: "EsperancaVidaMulheres80"})
    df_mortalidade_antes40 = df.groupby(["Continente", "Year"], observed=False)[mortalidade_antes40].mean().reset_index().rename(columns={mortalidade_antes40: "MortalidadeAntes40"})
    df_mortalidade_antes60 = df.groupby(["Continente", "Year"], observed=False)[mortalidade_antes60].mean().reset_index().rename(columns={mortalidade_antes60: "MortalidadeAntes60"})
    df_mortalidade_entre15e50 = df.groupby(["Continente", "Year"], observed=False)[mortalidade_entre15e50].mean().reset_index().rename(columns={mortalidade_entre15e50: "MortalidadeEntre15e50"})
    df_taxa_migracao_liquida = df.groupby(["Continente", "Year"], observed=False)[taxa_migracao_liquida].sum().reset_index().rename(columns={taxa_migracao_liquida: "TaxaMigracaoLiquida"})
    df_mortalidade_entre15e50Homens = df.groupby(["Continente", "Year"], observed=False)[mortalidade_entre15e50Homens].mean().reset_index().rename(columns={mortalidade_entre15e50Homens: "MortalidadeEntre15e50Homens"})
    df_mortalidade_entre15e50Mulheres = df.groupby(["Continente", "Year"], observed=False)[mortalidade_entre15e50Mulheres].mean().reset_index().rename(columns={mortalidade_entre15e50Mulheres: "MortalidadeEntre15e50Mulheres"})
    
    return (df_pop, 
        df_dens, 
        df_racio, 
        df_cresc, 
        df_idade_media, 
        df_taxa_alteracao_natural, 
        df_nascimentos, 
        df_obitos, 
        df_esperanca_vida, 
        df_esperanca_vida_homens80, 
        df_esperanca_vida_mulheres80, 
        df_mortalidade_antes40, 
        df_mortalidade_antes60, 
        df_mortalidade_entre15e50, 
        df_taxa_migracao_liquida,
        df_mortalidade_entre15e50Homens,
        df_mortalidade_entre15e50Mulheres)
