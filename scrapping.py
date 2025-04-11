import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import logging
import re
import time
import random
import signal
import sys
from tqdm import tqdm  # Para barras de progreso
from datetime import datetime  # Para timestamps en los archivos

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

class FragranticaScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.perfumes = []

    def get_page(self, url, max_retries=5):
        """Obtiene una página con manejo de errores y reintentos"""
        for attempt in range(max_retries):
            try:
                # Si no es el primer intento, esperar con backoff exponencial
                if attempt > 0:
                    wait_time = min(300, (2 ** attempt) * 30)  # máximo 5 minutos
                    print(f"Reintento {attempt + 1}/{max_retries}. Esperando {wait_time} segundos...")
                    time.sleep(wait_time)

                response = self.session.get(url)
                
                # Manejar específicamente el error 429
                if response.status_code == 429:
                    # Obtener el tiempo de espera del header si está disponible
                    wait_time = int(response.headers.get('Retry-After', 300))
                    print(f"Recibido código 429. Esperando {wait_time} segundos...")
                    time.sleep(wait_time)
                    continue
                    
                response.raise_for_status()
                
                # Añadir un delay aleatorio entre requests exitosos
                time.sleep(random.uniform(5, 10))
                
                return BeautifulSoup(response.content, 'html.parser')
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"Error fetching page {url}: {e}")
                    return None
                print(f"Error en intento {attempt + 1}: {str(e)}")

        return None

    def extract_brand_from_url(self, url):
        """Extrae la marca de la URL del perfume"""
        try:
            # Buscar el patrón /perfume/MARCA/
            match = re.search(r'/perfume/([^/]+)/', url)
            if match:
                # Reemplazar guiones por espacios y decodificar caracteres especiales
                brand = match.group(1).replace('-', ' ')
                return brand
            return None
        except Exception as e:
            print(f"Error extracting brand from URL: {e}")
            return None

    def extract_accords(self, soup):
        """Función específica para extraer todos los acordes"""
        try:
            accords = []
            
            # Imprimir todas las clases grid-x encontradas
            grid_containers = soup.find_all('div', class_='grid-x')
            # print(f"\nEncontradas {len(grid_containers)} contenedores grid-x")
            
            for i, grid in enumerate(grid_containers):
                # print(f"\nContenido del grid-x #{i+1}:")
                # print(grid.prettify())
                
                # Intentar encontrar acordes en este grid
                accord_elements = grid.find_all('div', class_=['accord-bar', 'accord-box'])
                if accord_elements:
                    # print(f"Encontrados {len(accord_elements)} elementos de acordes en grid #{i+1}")
                    for accord in accord_elements:
                        if accord.text.strip():
                            accords.append(accord.text.strip())
                            # print(f"Acorde añadido: {accord.text.strip()}")

            # Si no encontramos nada, intentar con otros selectores
            # if not accords:
            #     print("\nIntentando con selectores alternativos...")
                
            #     # Intentar encontrar directamente los accord-bar
            #     direct_accords = soup.find_all('div', class_='accord-bar')
            #     print(f"Encontrados {len(direct_accords)} accord-bar directamente")
                
            #     for accord in direct_accords:
            #         text = accord.text.strip()
            #         if text:
            #             accords.append(text)
            #             print(f"Acorde añadido (método directo): {text}")

            # print(f"\nTotal de acordes encontrados: {len(accords)}")
            return list(dict.fromkeys(accords))
                
        except Exception as e:
            print(f"Error extracting accords: {e}")
            return []
    
    def extract_rating(self, soup):
        """Función específica para extraer rating usando el selector correcto"""
        try:
            rating_data = {
                'rating': None
            }

            # Buscar el rating con el selector correcto
            rating_element = soup.find('span', attrs={'itemprop': 'ratingValue'})
            if rating_element:
                try:
                    rating = float(rating_element.text.strip())
                    rating_data['rating'] = rating
                    # print(f"Rating encontrado: {rating}")
                except ValueError:
                    print("Error convirtiendo rating a número")

            # También podemos buscar dentro de info-note
            info_note = soup.find('div', class_='info-note')
            if info_note:
                rating_element = info_note.find('span', attrs={'itemprop': 'ratingValue'})
                if rating_element and not rating_data['rating']:
                    try:
                        rating = float(rating_element.text.strip())
                        rating_data['rating'] = rating
                        # print(f"Rating encontrado en info-note: {rating}")
                    except ValueError:
                        print("Error convirtiendo rating a número")

            return rating_data

        except Exception as e:
            print(f"Error extracting rating: {e}")
            return {'rating': None}
    def extract_piramide_olfativa(self, soup):
        """Extrae la pirámide olfativa basada en el div id='pyramid'"""
        try:
            piramide = {
                'notas_de_salida': [],
                'notas_de_corazon': [],
                'notas_de_base': []
            }

            # Encontrar el contenedor principal de la pirámide
            pyramid_div = soup.find('div', id='pyramid')
            if pyramid_div:
                # Encontrar todas las secciones h4 que contienen los títulos
                sections = pyramid_div.find_all('h4')
                
                for section in sections:
                    # Obtener el título de la sección
                    title = section.find('b').text.strip()
                    
                    # Encontrar el pyramid-level siguiente
                    pyramid_level = section.find_next('pyramid-level')
                    if pyramid_level:
                        # Encontrar todos los div que contienen notas
                        note_divs = pyramid_level.find_all('div', style=lambda x: x and 'margin: 0.2rem' in x)
                        
                        # Extraer los nombres de las notas
                        notes = []
                        for div in note_divs:
                            # El texto de la nota está después del <a>
                            note_text = div.find('a').next_sibling
                            if note_text and note_text.strip():
                                notes.append(note_text.strip())
                        
                        # Asignar las notas a la sección correspondiente
                        if 'Salida' in title:
                            piramide['notas_de_salida'] = notes
                            
                        elif 'Top' in title:
                            piramide['notas_de_salida'] = notes
                            
                        elif 'Middle' in title:
                            piramide['notas_de_corazon'] = notes
                            
                        elif 'Corazón' in title:
                            piramide['notas_de_corazon'] = notes
                            
                        elif 'Base' in title:
                            piramide['notas_de_base'] = notes
                            

            return piramide

        except Exception as e:
            print(f"Error extracting olfactory pyramid: {e}")
            return {
                'notas_de_salida': [],
                'notas_de_corazon': [],
                'notas_de_base': []
            }
    def extract_longevidad(self, soup):
        """Extrae la longevidad del perfume"""
        try:
            longevidad_div = soup.find('div', class_='longevity-box')
            if longevidad_div:
                return longevidad_div.text.strip()
            return None
        except Exception as e:
            print(f"Error extracting longevity: {e}")
            return None

    def extract_ano(self, soup):
        """Extrae el año del perfume"""
        try:
            # El año suele estar en un span con itemprop="dateCreated"
            year_span = soup.find('span', attrs={'itemprop': 'dateCreated'})
            if year_span:
                return year_span.text.strip()
            return None
        except Exception as e:
            print(f"Error extracting year: {e}")
            return None

    def extract_genero(self, soup):
        """Extrae el género del perfume"""
        try:
            # El género suele estar en un elemento con class="gender-box"
            gender_div = soup.find('div', class_='gender-box')
            if gender_div:
                return gender_div.text.strip()
            return None
        except Exception as e:
            print(f"Error extracting gender: {e}")
            return None

    def get_all_perfume_urls(self):
        """Obtiene URLs de perfumes buscando por letras individuales"""
        try:
            all_urls = set()  # Usamos set para evitar duplicados
            caracteres = list('a')  # Solo letras
            
            for letra in tqdm(caracteres, desc="Buscando por letras"):
                try:
                    # Construir URL de búsqueda
                    url = f"https://www.fragrantica.es/buscar/?query={letra}"
                    print(f"\nBuscando perfumes con la letra: {letra}")
                    print(f"URL: {url}")
                    
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Buscar los enlaces a perfumes
                    perfume_links = soup.find_all('a', href=lambda x: x and '/perfume/' in x)
                    
                    # Corregir la construcción de URLs
                    new_urls = set()
                    for link in perfume_links:
                        href = link.get('href')
                        if href.startswith('http'):
                            new_urls.add(href)  # Si ya es una URL completa
                        elif href.startswith('//'):
                            new_urls.add(f"https:{href}")  # Si es una URL relativa al protocolo
                        else:
                            new_urls.add(f"https://www.fragrantica.es{href}")  # Si es una ruta relativa
                    
                    previous_count = len(all_urls)
                    all_urls.update(new_urls)
                    
                    print(f"Encontrados {len(new_urls)} perfumes para la letra {letra}")
                    print(f"Total acumulado: {len(all_urls)}")
                    
                    # Imprimir algunas URLs de ejemplo para verificar
                    print("\nEjemplos de URLs encontradas:")
                    for url in list(new_urls)[:3]:
                        print(url)
                    
                    # Esperar entre requests
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    logger.error(f"Error procesando letra {letra}: {e}")
                    continue
            
            return list(all_urls)
        except Exception as e:
            logger.error(f"Error getting perfume URLs: {e}")
            return []




    def parse_perfume_details(self, url):
        try:
            soup = self.get_page(url)
            if not soup:
                return None

            # Extraer título y género
            title = soup.select_one('h1[itemprop="name"]')
            if title:
                main_title = title.find(text=True, recursive=False)
                main_title = main_title.strip() if main_title else ""
                small_text = title.find('small')
                gender = small_text.text.strip() if small_text else ""
                gender = gender.replace("para ", "").strip()
            else:
                main_title = ""
                gender = ""

            # Extraer marca
            brand = self.extract_brand_from_url(url)
            
            # Extraer acordes principales
            acordes = list(set(self.extract_accords(soup)))  # Usando set para evitar duplicados

            # Extraer rating y votos
            rating_info = self.extract_rating(soup)

            # Extraer pirámide olfativa
            piramide = self.extract_piramide_olfativa(soup)

            # Extraer información adicional
            longevidad = self.extract_longevidad(soup)
            ano = self.extract_ano(soup)
            genero = self.extract_genero(soup)

            perfume_data = {
                'url': url,
                'marca': brand,
                'nombre': main_title,
                'genero (desde el titulo)': gender,
                # 'genero': genero,
                # 'ano': ano,
                'acordes_principales': acordes,
                'rating': rating_info['rating'],
                'piramide_olfativa': piramide,
                # 'longevidad': longevidad,
                
            }
            
            return perfume_data

        except Exception as e:
            logger.error(f"Error parsing perfume {url}: {e}")
            return None
    def save_to_json(self, filename='fragrantica_perfumes.json'):
        """Guarda los resultados en un archivo JSON"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.perfumes, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(self.perfumes)} perfumes to {filename}")
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")

    def save_to_csv(self, filename='fragrantica_perfumes.csv'):
        """Guarda los resultados en un archivo CSV"""
        try:
            df = pd.DataFrame(self.perfumes)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Saved {len(self.perfumes)} perfumes to {filename}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")



# Ejemplo de uso
if __name__ == "__main__":
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        scraper = FragranticaScraper()
        
        # Obtener URLs
        print("Iniciando búsqueda de URLs por letra...")
        all_perfume_urls = scraper.get_all_perfume_urls()
        
        # Mostrar resultados iniciales
        print(f"\nTotal de URLs encontradas: {len(all_perfume_urls)}")
        if len(all_perfume_urls) > 0:
            print("\nPrimeras 5 URLs encontradas:")
            for url in list(all_perfume_urls)[:5]:
                print(url)
        
        # Procesar los perfumes encontrados
        for i, url in enumerate(all_perfume_urls):
            try:
                print(f"\nProcesando perfume {i+1} de {len(all_perfume_urls)}")
                perfume_data = scraper.parse_perfume_details(url)
                
                if perfume_data:
                    scraper.perfumes.append(perfume_data)
                    
                    # Backup cada 50 perfumes
                    if len(scraper.perfumes) % 50 == 0:
                        backup_file = f'backup_perfumes_{timestamp}_{len(scraper.perfumes)}.json'
                        scraper.save_to_json(backup_file)
                        print(f"Backup guardado: {backup_file}")
                
                time.sleep(random.uniform(5, 10))
                
            except Exception as e:
                logger.error(f"Error procesando {url}: {e}")
                continue
        
        # Guardar resultados finales
        scraper.save_to_json(f'perfumes_final_{timestamp}.json')
        scraper.save_to_csv(f'perfumes_final_{timestamp}.csv')
        print(f"\nProceso completado. Total de perfumes scrapeados: {len(scraper.perfumes)}")
        
    except KeyboardInterrupt:
        print("\nInterrupción detectada. Guardando progreso...")
        scraper.save_to_json(f'interrupted_perfumes_{timestamp}.json')
    except Exception as e:
        logger.error(f"Error general: {e}")
        if hasattr(scraper, 'perfumes') and scraper.perfumes:
            scraper.save_to_json(f'error_perfumes_{timestamp}.json')
