import pandas as pd
import numpy as np
import gc
import os
import sys

def get_memory_usage():
    """
    Approximation simple de l'utilisation mémoire sans psutil
    """
    try:
        # Sur les systèmes Linux, lire /proc/self/status
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    memory_kb = int(line.split()[1])
                    return memory_kb / 1024  # Conversion en MB
    except:
        pass
    return 0

def check_file_info(filepath):
    """
    Vérifie les informations de base du fichier
    """
    if not os.path.exists(filepath):
        print(f"ERREUR : Le fichier {filepath} n'existe pas")
        return False
    
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"Taille du fichier : {size_mb:.1f} MB")
    return True

def peek_file_structure(filepath, nrows=5):
    """
    Examine la structure du fichier sans le charger entièrement
    """
    print("Examen de la structure du fichier...")
    try:
        sample_df = pd.read_csv(filepath, sep='\t', nrows=nrows)
        print(f"Colonnes totales : {len(sample_df.columns)}")
        print(f"Premières colonnes : {list(sample_df.columns[:10])}")
        return sample_df.columns.tolist()
    except Exception as e:
        print(f"Erreur lors de l'examen : {e}")
        return None

def extract_subtable_optimized(filepath, fields, chunksize=5000):
    """
    Extrait les colonnes par chunks pour économiser la mémoire
    """
    # Identifier les colonnes d'intérêt
    print("Identification des colonnes...")
    all_columns = peek_file_structure(filepath, nrows=1)
    if all_columns is None:
        return None
    
    columns_to_extract = []
    
    # Chercher la colonne ID
    id_cols = [col for col in all_columns if col in ['f.eid', 'eid', 'ID']]
    if id_cols:
        columns_to_extract.append(id_cols[0])
        print(f"Colonne ID trouvée : {id_cols[0]}")
    else:
        print("ATTENTION : Aucune colonne ID trouvée, utilisation de la première colonne")
        columns_to_extract.append(all_columns[0])
    
    # Chercher les colonnes pour chaque champ
    for field in fields:
        matching_cols = [col for col in all_columns if col.startswith(field)]
        columns_to_extract.extend(matching_cols)
        print(f"Champ {field} : {len(matching_cols)} colonnes trouvées")
    
    # Supprimer les doublons tout en préservant l'ordre
    seen = set()
    columns_to_extract = [col for col in columns_to_extract if not (col in seen or seen.add(col))]
    
    print(f"Colonnes à extraire : {len(columns_to_extract)}")
    
    if len(columns_to_extract) <= 1:
        print("ERREUR : Aucune colonne de données trouvée")
        return None
    
    # Lecture par chunks
    print("Lecture des données par chunks...")
    chunks = []
    total_rows = 0
    
    try:
        chunk_iterator = pd.read_csv(filepath, sep='\t', chunksize=chunksize, 
                                   usecols=columns_to_extract, low_memory=False)
        
        for i, chunk in enumerate(chunk_iterator):
            chunks.append(chunk)
            total_rows += len(chunk)
            
            if (i + 1) % 10 == 0:  # Affichage tous les 10 chunks
                memory_mb = get_memory_usage()
                print(f"Chunks lus : {i+1}, Lignes : {total_rows}, Mémoire : {memory_mb:.1f} MB")
            
            # Limite de sécurité pour éviter les débordements mémoire
            if len(chunks) > 200:  # Plus de 1M de lignes avec chunks de 5000
                print("Assemblage intermédiaire pour économiser la mémoire...")
                temp_result = pd.concat(chunks, ignore_index=True)
                chunks = [temp_result]
                gc.collect()
        
        # Assemblage final
        print("Assemblage final des données...")
        result = pd.concat(chunks, ignore_index=True)
        
        # Nettoyage
        del chunks
        gc.collect()
        
        print(f"Données extraites : {len(result)} lignes, {len(result.columns)} colonnes")
        return result
        
    except Exception as e:
        print(f"Erreur lors de la lecture : {e}")
        return None

def relabel_columns(df, fields, array_length, instances, labels):
    """
    Renomme les colonnes selon la logique spécifiée
    """
    if df is None or len(df.columns) == 0:
        return df
    
    new_names = ['ID']  # Premier nom pour l'ID
    current_col = 1  # Index de la colonne courante (après ID)
    
    for i, field in enumerate(fields):
        if i >= len(labels):
            break
            
        base_label = labels[i]
        arr_len = array_length[i] if i < len(array_length) else 1
        inst = instances[i] if i < len(instances) else 1
        
        # Compter les colonnes correspondant à ce champ
        matching_cols = [col for col in df.columns[1:] if col.startswith(field)]
        num_cols = len(matching_cols)
        
        if num_cols == 0:
            print(f"Attention : Aucune colonne pour {field}")
            continue
        
        # Générer les noms selon la logique
        if arr_len == 1 and inst == 1:
            new_names.append(base_label)
        elif arr_len > 1:
            # Pour les composantes principales (PC1, PC2, etc.)
            for j in range(min(num_cols, arr_len)):
                new_names.append(f"{base_label}{j+1}")
        elif inst > 1:
            # Pour les instances multiples
            for j in range(min(num_cols, inst)):
                new_names.append(f"{base_label}_inst{j+1}")
        else:
            for j in range(num_cols):
                new_names.append(f"{base_label}_{j+1}")
    
    # Ajuster si nous n'avons pas assez de noms
    while len(new_names) < len(df.columns):
        new_names.append(f"Col_{len(new_names)}")
    
    # Tronquer si nous avons trop de noms
    new_names = new_names[:len(df.columns)]
    
    return new_names

def main():
    """
    Fonction principale optimisée pour Narval
    """
    
    ################# CONFIGURATION ##################
    
    pathfile_in = "ukb8045.r.tab"
    
    fields = ['f.31.', 'f.21000.', 'f.21003.', 'f.22001.', 'f.22006.', 'f.22009.', 'f.22010.', 'f.22018.']
    array_length = [1, 1, 1, 1, 1, 40, 1, 1]
    instances = [1, 3, 3, 1, 1, 1, 1, 1]
    labels = ["Sex", "Ethnic_backgr", "Age_at_Visit", "Genetic_sex",
              "Gen_ethnic_grp", "PC", "Geno_analys_exclns", "Relat_exclns"]
    
    filepath_out = "demographic_data.tsv"
    
    ############################### EXÉCUTION ############################################
    
    print("=== Extraction de données démographiques UK Biobank sur Narval ===")
    print(f"Mémoire initiale : {get_memory_usage():.1f} MB")
    
    # Vérifications préliminaires
    if not check_file_info(pathfile_in):
        return
    
    try:
        # Extraction des données
        print("\n--- Phase 1: Extraction des données ---")
        df = extract_subtable_optimized(pathfile_in, fields, chunksize=3000)
        
        if df is None:
            print("ERREUR : Échec de l'extraction des données")
            return
        
        print(f"Mémoire après extraction : {get_memory_usage():.1f} MB")
        
        # Renommage des colonnes
        print("\n--- Phase 2: Renommage des colonnes ---")
        new_column_names = relabel_columns(df, fields, array_length, instances, labels)
        
        if len(new_column_names) == len(df.columns):
            df.columns = new_column_names
            print("Renommage réussi")
        else:
            print(f"Attention : {len(new_column_names)} noms vs {len(df.columns)} colonnes")
        
        print(f"Colonnes finales : {list(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
        
        # Sauvegarde
        print("\n--- Phase 3: Sauvegarde ---")
        print(f"Écriture vers {filepath_out}...")
        
        df.to_csv(filepath_out, sep='\t', index=False)
        
        # Vérification du fichier de sortie
        if os.path.exists(filepath_out):
            size_mb = os.path.getsize(filepath_out) / (1024 * 1024)
            print(f"Fichier créé avec succès : {size_mb:.1f} MB")
        
        print("\n=== TERMINÉ AVEC SUCCÈS ===")
        
    except MemoryError:
        print("ERREUR MÉMOIRE : Réduisez le chunk_size ou demandez plus de RAM")
    except KeyboardInterrupt:
        print("Interruption par l'utilisateur")
    except Exception as e:
        print(f"ERREUR INATTENDUE : {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        gc.collect()
        print(f"Mémoire finale : {get_memory_usage():.1f} MB")

if __name__ == "__main__":
    main()
