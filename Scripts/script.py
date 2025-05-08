import os

# cancellare tutti i file da una cartella con un determinato nome
def delete_files_in_folder(folder_path, base_name, n):
    """
    Deletes all files in a specified folder that match a given file name.
    Args:
        folder_path (str): The path to the folder containing the files.
        file_name (str): The name of the files to delete.
    """
    for filename in os.listdir(folder_path):
        #inserisco l'index alla fine del nome del file
        if filename.startswith(base_name) and filename.endswith(".json"):
            # estraggo l'index del file
            index = filename.split("_")[-1].split(".")[0]
            # se l'index è maggiore di 12 lo cancello
            if int(index) > n:
                # cancello il file
                file_path = os.path.join(folder_path, filename)
                os.remove(file_path)
                print(f"Deleted {file_path}")
            else:
                print(f"No files starting with {filename} found in {folder_path}")

#delete_files_in_folder("Data/raw/transactions", "BitZillions.com_transactions", 12)
