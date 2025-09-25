import pandas as pd
import numpy as np

def filter_europeans(input_file, output_file):
    """
    Filtre les participants européens des données UK Biobank
    """
    
    print("=== Filtrage des participants européens ===")
    
    # Lecture des données
    print("Lecture des données démographiques...")
    df = pd.read_csv(input_file, sep='\t')
    print(f"Données initiales : {len(df)} participants, {len(df.columns)} colonnes")
    
    # Affichage des colonnes pour vérification
    print(f"Colonnes disponibles : {list(df.columns)}")
    
    # Identification des colonnes ethniques
    ethnic_cols = [col for col in df.columns if 'Ethnic' in col or 'ethnic' in col]
    print(f"Colonnes ethniques trouvées : {ethnic_cols}")
    
    # Statistiques avant filtrage
    print("\n--- Statistiques avant filtrage ---")
    for col in ethnic_cols:
        if col in df.columns:
            print(f"\n{col}:")
            value_counts = df[col].value_counts(dropna=False)
            print(value_counts.head(10))
    
    # Définition des codes pour les Européens
    # Dans UK Biobank :
    # Field 21000 (Ethnic background) : 1001 = British, 1002 = Irish, 1003 = Any other white background
    # Field 22006 (Genetic ethnic grouping) : 1 = Caucasian
    
    european_codes_ethnic = [1001, 1002, 1003]  # Codes pour ethnicité auto-rapportée
    european_codes_genetic = [1]  # Code pour ethnicité génétique
    
    # Création du masque de filtrage
    mask = pd.Series([False] * len(df))
    
    # Filtrage sur l'ethnicité auto-rapportée (field 21000)
    ethnic_backgr_cols = [col for col in df.columns if col.startswith('Ethnic_backgr')]
    if ethnic_backgr_cols:
        print(f"\nUtilisation des colonnes ethnicité auto-rapportée : {ethnic_backgr_cols}")
        for col in ethnic_backgr_cols:
            # Participants avec codes européens dans n'importe quelle instance
            european_mask = df[col].isin(european_codes_ethnic)
            mask = mask | european_mask
            print(f"  {col}: {european_mask.sum()} participants européens")
    
    # Filtrage additionnel sur l'ethnicité génétique si disponible
    gen_ethnic_col = [col for col in df.columns if col.startswith('Gen_ethnic_grp')]
    if gen_ethnic_col:
        print(f"\nUtilisation de la colonne ethnicité génétique : {gen_ethnic_col}")
        genetic_mask = df[gen_ethnic_col[0]].isin(european_codes_genetic)
        # Utilisation de l'intersection (AND) pour être plus strict
        if mask.any():
            mask = mask & (genetic_mask | df[gen_ethnic_col[0]].isna())
        else:
            mask = genetic_mask
        print(f"  Participants avec ethnicité génétique caucasienne : {genetic_mask.sum()}")
    
    # Application du filtre
    df_europeans = df[mask].copy()
    
    print(f"\n--- Résultats du filtrage ---")
    print(f"Participants européens identifiés : {len(df_europeans)}")
    print(f"Pourcentage : {len(df_europeans)/len(df)*100:.1f}%")
    
    # Vérification des résultats
    print("\n--- Vérification des résultats ---")
    for col in ethnic_cols:
        if col in df_europeans.columns:
            print(f"\n{col} (après filtrage):")
            value_counts = df_europeans[col].value_counts(dropna=False)
            print(value_counts.head(10))
    
    # Sauvegarde
    print(f"\nSauvegarde vers {output_file}...")
    df_europeans.to_csv(output_file, sep='\t', index=False)
    
    print(f"Fichier créé : {len(df_europeans)} participants européens")
    return df_europeans

def analyze_ethnic_distribution(df):
    """
    Analyse détaillée de la distribution ethnique
    """
    print("\n=== Analyse détaillée de la distribution ethnique ===")
    
    # Codes UK Biobank pour référence
    ethnicity_codes = {
        1: "White",
        1001: "British", 
        1002: "Irish",
        1003: "Any other white background",
        2001: "White and Black Caribbean",
        2002: "White and Black African", 
        2003: "White and Asian",
        2004: "Any other mixed background",
        3001: "Indian",
        3002: "Pakistani", 
        3003: "Bangladeshi",
        3004: "Any other Asian background",
        4001: "Caribbean",
        4002: "African",
        4003: "Any other Black background",
        5: "Chinese",
        6: "Other ethnic group",
        -1: "Do not know",
        -3: "Prefer not to answer"
    }
    
    ethnic_cols = [col for col in df.columns if 'Ethnic_backgr' in col]
    
    for col in ethnic_cols:
        print(f"\n--- {col} ---")
        value_counts = df[col].value_counts(dropna=False)
        
        for code, count in value_counts.items():
            description = ethnicity_codes.get(code, f"Code inconnu: {code}")
            percentage = (count / len(df)) * 100
            print(f"  {code}: {description} - {count} ({percentage:.1f}%)")

def main():
    """
    Fonction principale
    """
    
    # Fichiers d'entrée et de sortie
    input_file = "demographic_data.tsv"
    output_file = "european_participants.tsv"
    
    try:
        # Chargement et analyse des données
        print("Lecture des données pour analyse préliminaire...")
        df_sample = pd.read_csv(input_file, sep='\t', nrows=1000)
        analyze_ethnic_distribution(df_sample)
        
        # Filtrage des Européens
        df_europeans = filter_europeans(input_file, output_file)
        
        # Statistiques finales
        print(f"\n=== RÉSUMÉ ===")
        print(f"Fichier d'entrée : {input_file}")
        print(f"Fichier de sortie : {output_file}")
        print(f"Participants européens : {len(df_europeans)}")
        
        # Sauvegarde d'un fichier avec seulement les IDs européens
        ids_file = "european_participant_ids.txt"
        df_europeans['ID'].to_csv(ids_file, index=False, header=False)
        print(f"Liste des IDs européens : {ids_file}")
        
    except FileNotFoundError:
        print(f"ERREUR : Le fichier {input_file} n'existe pas.")
        print("Assurez-vous d'avoir d'abord exécuté le script d'extraction démographique.")
    except Exception as e:
        print(f"ERREUR : {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
