import requests
from bs4 import BeautifulSoup
import time
import tkinter as tk
from tkinter import messagebox, filedialog
from tkinter import ttk
import threading
import pandas as pd
import re
import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment
from openpyxl.worksheet.table import Table, TableStyleInfo
import webbrowser
import os
import sys

# Una funzione in grado di darmi il percorso per la cartella Risorse che andrò ad inserire all'interno dell'eseguibile
def resource_path(relative_path):
    """ Ottieni il percorso assoluto alla risorsa, funziona sia in fase di sviluppo che in fase di distribuzione """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

# Inizio a fare un filtraggio sull'html del sito
def extract_identifiers(page_url):
    print("Extracting identifiers from the page...")
    response = requests.get(page_url)
    response.raise_for_status()

# Interpreto il codice html tramite un parser, esso mi permetterà di comprenderne la struttura
    soup = BeautifulSoup(response.content, 'html.parser')

    identifiers = []
    table = soup.find('table', id='dt-select')

    if table:
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            cell_text = [cell.get_text(strip=True) for cell in cells]

# Filtro solo le righe che contengono "Italy"
            if "Italy" in cell_text:
                for cell in cells:
                    link = cell.find('a', href=True)
                    if link and link['href'].endswith('.csv'):
                        href = link['href']
# Estrai l'identificativo dal nome del file .csv
                        identifier = href.split('/')[-1].split('-')[0]
                        identifiers.append(identifier)
                        print(f"Identifier extracted: {identifier}")

    print(f"Found {len(identifiers)} identifiers.")
    return identifiers

# Cerco nella pagina solo i link .csv, così facendo mi accerto di star cercando dei link da downloadare
def fetch_csv_links(page_url, base_url, identifiers):
    print("Fetching CSV links from the page...")
    response = requests.get(page_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    csv_urls = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        print(f"Found link: {href}")
        if '.csv' in href and any(identifier in href for identifier in identifiers):
            print(f"Found CSV link: {href}")
            if not href.startswith('http'):
                full_url = requests.compat.urljoin(base_url, href)
                csv_urls.append(full_url)
            else:
                csv_urls.append(href)

    print(f"Found {len(csv_urls)} CSV links.")
    return csv_urls

# Scarico ciò che ho filtrato, unisco il tutto in un solo file.csv e tolgo le parti html in eccesso
# Infine sono pronto a inserirlo nel dataframe per iniziare a processarlo
def download_csv(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        content_type = response.headers.get('Content-Type')
        if 'text/csv' in content_type or 'application/csv' in content_type:
            print(f"Content type confirmed as CSV: {url}")
            return response.content.decode('latin-1')
        else:
            print(f"Content at {url} is not CSV.")
            return None
    except requests.RequestException as e:
        print(f"Errore nel download del file CSV: {url}. Eccezione: {e}")
        return None

def merge_csv(contents):
    return "\n".join(contents)

def extract_square_bracket_number(alert_message):
    match = re.search(r'\[(\d+)\]', alert_message)
    if match:
        return match.group(0)  # Include le parentesi (Brakcets)
    return None

def clean_alert_message(alert_message):
    number_with_brackets = extract_square_bracket_number(alert_message)
    if number_with_brackets:
        start_idx = alert_message.find(number_with_brackets)
        return alert_message[start_idx:]
    return alert_message

def process_csv_to_dataframe(csv_content):
    data_list = []
    lines = csv_content.splitlines()

    shortest_alerts = {}
    idx = 0

# Divido il file in diverse colonne che sono linee presenti nel file .csv
    while idx < len(lines):
        if not lines[idx].strip():
            idx += 1
            continue
# Divido in più colonne le righe trovate        
        eAlertSlNo_line = lines[idx].strip().split("\t")
        SRN_line = lines[idx + 1].strip().split("\t")
        HCFName_line = lines[idx + 3].strip().split("\t")
        Modality_line = lines[idx + 4].strip().split("\t")
        ModalityCfg_line = lines[idx + 5].strip().split("\t")

        try:
            eAlertSlNo = eAlertSlNo_line[1] if len(eAlertSlNo_line) > 1 else ''
            SRN = SRN_line[1] if len(SRN_line) > 1 else ''
            HCFName = HCFName_line[1] if len(HCFName_line) > 1 else ''
            Modality = Modality_line[1] if len(Modality_line) > 1 else ''
            ModalityCfg = ModalityCfg_line[1] if len(ModalityCfg_line) > 1 else ''
        except IndexError:
# Se c'è un errore ritorna un dataframe vuoto
            print(f"Errore nell'analizzare la riga: {lines[idx:idx+6]}")
            return pd.DataFrame()

        idx += 6

        DateTime_AlertType_AlertsSent = []
        while idx < len(lines) and lines[idx].count("\t") == 2:
            DateTime_AlertType_AlertsSent.append(lines[idx].strip().split("\t"))
            idx += 1

        alert_dict = {}
        for item in DateTime_AlertType_AlertsSent:
            if len(item) == 3:
                date_time, alert_type, alerts_sent = item
                if alerts_sent.strip().lower() == 'alertssent':
                    continue

                bracket_number = extract_square_bracket_number(alerts_sent)
                if bracket_number is None:
                    continue

                key = (bracket_number, alert_type)
                cleaned_alert_sent = clean_alert_message(alerts_sent)

                if bracket_number not in shortest_alerts or len(cleaned_alert_sent) < len(shortest_alerts[bracket_number]):
                    shortest_alerts[bracket_number] = cleaned_alert_sent

                if key not in alert_dict:
                    alert_dict[key] = {
                        "FirstDateTime": date_time,
                        "FirstAlertType": alert_type,
                        "FirstAlertsSent": cleaned_alert_sent,
                        "CountAlertsSent": 1
                    }
                else:
                    alert_dict[key]["CountAlertsSent"] += 1

        for key, details in alert_dict.items():
            bracket_number, alert_type = key
            data_list.append([
                eAlertSlNo,
                SRN,
                HCFName,
                Modality,
                ModalityCfg,
                details["FirstDateTime"],
                details["FirstAlertType"],
                shortest_alerts[bracket_number],
                details["CountAlertsSent"]
            ])

    df = pd.DataFrame(data_list, columns=[
        "eAlertSlNo", "SRN", "HCFName", "Modality", "ModalityCfg",
        "FirstDateTime", "FirstAlertType", "FirstAlertsSent", "CountAlertsSent"
    ])

    df.drop(columns=['Modality', 'ModalityCfg'], inplace=True)
    return df

def merge_with_report(df, report_file):
    try:
        report_df = pd.read_excel(report_file)
    except FileNotFoundError:
        print(f"Errore: il file {report_file} non esiste.")
        return None
    except Exception as e:
        print(f"Errore durante la lettura del file: {e}")
        return None

    # Stampiamo i nomi delle colonne per verificare che siano quelle attese
    print("Colonne del report:", report_df.columns)

    # Utilizzando gli indici corretti
    work_center_col_index = 0
    prodotto_installato_col_index = 2
    num_serie_col_index = 3
    livello_superiore_col_index = 4

# Verifico che il DataFrame abbia abbastanza colonne
    if report_df.shape[1] < 5:
        print(f"Errore: il file report ha solo {report_df.shape[1]} colonne. Sono richieste almeno 5 colonne.")
        return None

# Usa gli indici per selezionare le colonne
# Qui correggeremo potenziali errori di slicing o accesso con columns
    relevant_columns = [num_serie_col_index, work_center_col_index, prodotto_installato_col_index, livello_superiore_col_index]
    report_df = report_df.iloc[:, relevant_columns]

# Trasformo entrambe le colonne in stringhe per effettuare il merge correttamente
    df['eAlertSlNo'] = df['eAlertSlNo'].astype(str)
    report_df.iloc[:, num_serie_col_index] = report_df.iloc[:, num_serie_col_index].astype(str)

# Se la colonna 'Livello superiore' deve essere di tipo int, eseguo una conversione e gestisco gli errori
    try:
        report_df.iloc[:, 3] = report_df.iloc[:, 3].astype(int)
    except ValueError as e:
        print("Avviso: errore durante la conversione a int della colonna 'Livello superiore'. Ecco il problema:", e)
    
# Procedo con il merge dei DataFrame
    merged_df = df.merge(report_df, left_on='eAlertSlNo', right_on=report_df.columns[0], how='left')
    merged_df.drop(columns=report_df.columns[0], inplace=True)

# Creo la colonna District
    merged_df['District'] = merged_df[report_df.columns[1]].apply(lambda x: x[:-3] if isinstance(x, str) else x)
    
# Filtro le righe dove HCF Name è 'HOME'
    merged_df = merged_df[~merged_df['HCFName'].str.contains('HOME', na=False)]

# Modifico l'ordine delle colonne per includere 'Work Center' come prima colonna
    columns_order = [
        report_df.columns[1],  # Work Center
        'District',
        'eAlertSlNo',
        report_df.columns[2],  # Prodotto installato
        report_df.columns[3],  # Livello superiore
        'SRN',
        'HCFName',
        'FirstDateTime',
        'FirstAlertType',
        'FirstAlertsSent',
        'CountAlertsSent'
    ]
    merged_df = merged_df[columns_order]
    merged_df.sort_values(by='CountAlertsSent', ascending=False, inplace=True)

# Cambio i nomi delle colonne per il risultato finale
    merged_df.rename(columns={
        report_df.columns[1]: 'Work Center',
        'District': 'District',
        'eAlertSlNo': 'E-alert SRN',
        report_df.columns[2]: 'IP E-alert',
        report_df.columns[3]: 'IP MR',
        'SRN': 'SRN MR',
        'HCFName': 'HCF Name',
        'FirstDateTime': 'First Date Time',
        'FirstAlertType': 'Who Received',
        'FirstAlertsSent': 'Alert type',
        'CountAlertsSent': 'Count Alerts Sent'
    }, inplace=True)

    return merged_df
# Inizo del download deciso dall'utente

def start_download_and_process(year, week, report_file, status_label, progress_bar):
    page_id = f"{year}{week:02d}"
    page_url = f"https://davide5214.github.io/Dummy_for_Raw/"
    base_url = "https://davide5214.github.io/Dummy_for_Raw/"
    
    status_label.config(text="Working...") #Flavour text
    root.update_idletasks()

    start_time = time.time()

# Faccio finalmente uso delle funzioni prima create
    identifiers = extract_identifiers(page_url)
    if identifiers is None or not identifiers:
        messagebox.showerror("Errore", "Nessun identificatore trovato.")
        return

    csv_links = fetch_csv_links(page_url, base_url, identifiers)
    if csv_links is None:
        messagebox.showerror("Errore", "Errore fetching CSV links.")
        return

# Genero la barra di avanzamento
    csv_contents = []
    progress_bar["value"] = 0
    progress_bar["maximum"] = len(csv_links)

    for csv_link in csv_links:
        csv_content = download_csv(csv_link)
        if csv_content:
            csv_contents.append(csv_content)
        progress_bar["value"] += 1
        root.update_idletasks()

    merged_content = merge_csv(csv_contents)
# Unisco il tutto con il report
    df = process_csv_to_dataframe(merged_content)
    if df is not None and not df.empty:
        df_merged = merge_with_report(df, report_file)
        if df_merged is not None:
            output_file = f"Log_E-alert_WK{week:02d}.xlsx"
            df_merged.to_excel(output_file, index=False, engine='openpyxl')

            wb = load_workbook(output_file)
            ws = wb.active
            for col in ws.columns:
                for cell in col:
                    if cell.column_letter == 'J':  # Controlla che la colonna sia J dato che sarà quella più lunga e problematica
                        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
                    else:
                        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=False)
                        cell.font = Font(name='Calibri', size=11)

            column_widths = {
                "A": 20, "B": 15, "C": 15, "D": 25, "E": 25, "F": 10, "G": 46, "H": 22, "I": 16, "J": 83, "K": 15
            }

            for col_letter, width in column_widths.items():
                ws.column_dimensions[col_letter].width = width

            tab = Table(displayName="DataTable", ref=ws.dimensions)
            style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False,
                                   showLastColumn=False, showRowStripes=True, showColumnStripes=True)
            tab.tableStyleInfo = style
            ws.add_table(tab)
            wb.save(output_file)
            messagebox.showinfo("Successo", f"Processing completato. Il file XLSX {output_file} è stato creato.")
    else:
        messagebox.showwarning("Attenzione", "Nessun CSV valido da processare.")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Tempo di esecuzione: {execution_time} secondi")

    status_label.config(text="Fatto.")
    progress_bar["value"] = len(csv_links)
    root.update_idletasks()

def open_file():
# Ottengo il percorso della directory dello script
    full_path = resource_path(os.path.join("Risorse", "messaggi e alert magneti rio.htm"))
    print(f"Tentativo di apertura file: {full_path}")

# Apro il file con la tabella di spiegazione nel browser
    webbrowser.open(f'file:///{full_path}')

def open_readme():
# Ottengo il percorso della directory dello script
    full_path = resource_path(os.path.join("Risorse", "ReadMe.pdf"))
    print(f"Tentativo di apertura Readme: {full_path}")

# Apro il file del tutorial nel browser
    webbrowser.open(f'file:///{full_path}')

def create_interface():
    global root
    root = tk.Tk()
    root.title("Download Log E-alert and Process")
    root.geometry("600x360+300+100")
    
# Creo un menu
    menu = tk.Menu(root)
    root.config(menu=menu)
    
# Aggiungo un menu user manual con opzione per aprire il file .HTM
    user_menu = tk.Menu(menu, tearoff=0)
    menu.add_cascade(label="Manuale Utente", menu=user_menu)
    user_menu.add_command(label="Spiegazione allarmi E-alert", command=open_file)

# Aggiungo un menu 'Aiuto' con opzione per aprire il tutorial dell'applicativo
    help_menu = tk.Menu(menu, tearoff=0)
    menu.add_cascade(label="Aiuto", menu=help_menu)
    help_menu.add_command(label="Istruzioni", command=open_readme)

    title_label = tk.Label(root, text="Remote e-Alert Workspot", font=("Helvetica", 16, "bold"), fg="blue")
# Uso pady per distanziare il titolo dagli altri widget
    title_label.pack(pady=(10, 0))
    
    padding_options = {'padx': 10, 'pady': 10}

    frame = tk.Frame(root)
    frame.pack(pady=10)

    tk.Label(frame, text="Anno di interesse").grid(row=0, column=0, **padding_options)
    year_entry = tk.Entry(frame, width=10)
    year_entry.grid(row=0, column=1, **padding_options)

    tk.Label(frame, text="Settimana di interesse").grid(row=0, column=2, **padding_options)
    week_entry = tk.Entry(frame, width=5)
    week_entry.grid(row=0, column=3, **padding_options)

    def select_report_file():
        global report_file
        report_file = filedialog.askopenfilename(
            title="Seleziona File XLSX del report", 
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        report_label.config(text=f"File XLSX selezionato: {report_file}")

    report_button = tk.Button(
        root, 
        text="Seleziona il file: 'RAW - MR IP Merger.xlsx' che si trova in 'Risorse'", 
        command=select_report_file
    )
    report_button.pack(pady=10)

    report_label = tk.Label(root, text="File XLSX selezionato: Nessuno")
    report_label.pack(pady=10)

    progress_bar = ttk.Progressbar(root, orient="horizontal", length=400, mode="determinate")
    progress_bar.pack(pady=10)

    status_label = tk.Label(root, text="", anchor='w')
    status_label.pack(pady=10)
    
# Programmo i pulsanti definendo gli errori e rendo possibile l'avvio
    def on_start():
        year = year_entry.get()
        week = week_entry.get()
        if year.isdigit() and week.isdigit() and len(year) == 4 and 1 <= int(week) <= 52:
            if report_file:
                threading.Thread(
                    target=start_download_and_process, 
                    args=(int(year), int(week), report_file, status_label, progress_bar)
                ).start()
            else:
                messagebox.showerror("Errore", "Specifica il report XLSX.")
        else:
            messagebox.showerror("Errore", "Anno o settimana non validi.")

    start_button = tk.Button(root, text="Avvia Download e Processamento", command=on_start)
    start_button.pack(pady=10)

    powered_label = tk.Label(root, text="Powered by Davide Salvia", anchor='se')
    powered_label.pack(side="right", padx=10, pady=10)

    version_label = tk.Label(root, text="Version 3.0.3 from https://davide5214.github.io/Dummy_for_Raw/", anchor='sw')
    version_label.pack(side="left", padx=10, pady=10)

    root.mainloop()

if __name__ == "__main__":
    create_interface()