import os
import pandas as pd
import time
from selenium import webdriver

csv_path = 'data/countries_profiles.csv'
save_dir = 'data/htmls'
os.makedirs(save_dir, exist_ok=True)

df = pd.read_csv(csv_path, sep=';')

# Configurando o ChromeDriver (baixe e coloque no PATH, ou use webdriver-manager)
# Mais seguro: webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver = webdriver.Chrome()

for idx, row in df.iterrows():
    slug = str(row.get('slug', '')).strip()
    if not slug or slug.lower() == 'nan':
        continue

    url = f'https://www.tasteatlas.com/{slug}'
    print(f'Downloading {slug}: {url}')
    try:
        driver.get(url)
        time.sleep(3)  # Aguarda o carregamento

        html = driver.page_source
        html_path = os.path.join(save_dir, f'{slug}.html')
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
    except Exception as e:
        print(f'Failed for {slug}: {e}')
driver.quit()