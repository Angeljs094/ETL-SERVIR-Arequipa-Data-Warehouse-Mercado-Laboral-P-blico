import os
import time
import pandas as pd
from datetime import date

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

SELENIUM_URL = os.getenv("SELENIUM_REMOTE_URL", "http://selenium-chrome:4444/wd/hub")

# ── Mapeo: etiqueta en pantalla → nombre de columna en BD ────
FIELD_MAP = {
    # vista de lista (tarjeta)
    "ubicación"                    : "ubicacion",
    "ubicacion"                    : "ubicacion",
    "remuneración"                 : "remuneracion",
    "remuneracion"                 : "remuneracion",
    "cantidad de vacantes"         : "cantidad_vacantes",
    "vacantes"                     : "cantidad_vacantes",
    "fecha inicio"                 : "fecha_inicio_publicacion",
    "fecha fin"                    : "fecha_fin_publicacion",
    "fecha inicio publicación"     : "fecha_inicio_publicacion",
    "fecha fin publicación"        : "fecha_fin_publicacion",
    "fecha inicio publicacion"     : "fecha_inicio_publicacion",
    "fecha fin publicacion"        : "fecha_fin_publicacion",
    # página de detalle
    "n° convocatoria"              : "numero_convocatoria",
    "n° de convocatoria"           : "numero_convocatoria",
    "numero convocatoria"          : "numero_convocatoria",
    "experiencia"                  : "experiencia",
    "experiencia laboral"          : "experiencia",
    "formación académica"          : "formacion_academica_perfil",
    "formacion academica"          : "formacion_academica_perfil",
    "formación académica/perfil"   : "formacion_academica_perfil",
    "formacion academica/perfil"   : "formacion_academica_perfil",
    "perfil"                       : "formacion_academica_perfil",
    "especialización"              : "especializacion",
    "especializacion"              : "especializacion",
    "conocimientos"                : "conocimiento",
    "conocimiento"                 : "conocimiento",
    "competencias"                 : "competencias",
    "competencias conductuales"    : "competencias",
}

def _mapear(etiqueta: str) -> str:
    key = etiqueta.lower().replace(":", "").strip()
    return FIELD_MAP.get(key, key)


# ── Driver ───────────────────────────────────────────────────
def init_driver():
    options = webdriver.ChromeOptions()

    # 🔥 Headless moderno
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")  # Chrome en contenedor pequeño

    # 🚀 Evitar detección y popups
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-images")
    
    

    # ⏱ Implicit wait más corto, confiamos en WebDriverWait puntual
    driver = webdriver.Remote(
        command_executor=SELENIUM_URL,
        options=options,
    )
    driver.implicitly_wait(2)  # antes estaba en 5

    return driver


# ── Selección de departamento ────────────────────────────────
def seleccionar_departamento(driver, departamento="AREQUIPA"):
    wait = WebDriverWait(driver, 15)

    combo_label = wait.until(
        EC.element_to_be_clickable((By.ID, "frmLstOfertsLabo:cboDep_label"))
    )
    driver.execute_script("arguments[0].click();", combo_label)

    opciones = wait.until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "ul.ui-selectonemenu-items li")
        )
    )
    for opcion in opciones:
        if opcion.text.strip().upper() == departamento.upper():
            driver.execute_script("arguments[0].click();", opcion)
            break

    boton_buscar = wait.until(
        EC.element_to_be_clickable((By.ID, "frmLstOfertsLabo:j_idt42"))
    )
    driver.execute_script("arguments[0].click();", boton_buscar)
    time.sleep(2)

# ── Extraer tarjetas de la lista ─────────────────────────────
def extraer_convocatorias(driver):
    """
    Campos visibles en la tarjeta de lista:
    título, lugar, y pares sub-titulo / detalle-sp
    (ubicación, remuneración, vacantes, fechas...).
    """
    convocatorias = []
    ofertas = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "div.col-sm-12.cuadro-vacantes")
        )
    )

    for oferta in ofertas:
        titulo = oferta.find_element(
            By.CSS_SELECTOR, "div.titulo-vacante label"
        ).text.strip()
        lugar = oferta.find_element(
            By.CSS_SELECTOR, "div.nombre-entidad span.detalle-sp"
        ).text.strip()

        registro = {"titulo": titulo, "lugar": lugar}

        etiquetas = oferta.find_elements(By.CSS_SELECTOR, "span.sub-titulo")
        valores   = oferta.find_elements(By.CSS_SELECTOR, "span.detalle-sp")

        # el primer detalle-sp es "lugar", los siguientes son los campos
        for etiq, val in zip(etiquetas, valores[1:]):
            col = _mapear(etiq.text)
            registro[col] = val.text.strip()

        convocatorias.append(registro)

    return convocatorias

# ── Navegar al detalle ───────────────────────────────────────
def entrar_ver_mas(driver, index):
    wait = WebDriverWait(driver, 5)
    boton = wait.until(
        EC.element_to_be_clickable(
            (By.ID, f"frmLstOfertsLabo:idPnlRepeatPuestos:{index}:j_idt71")
        )
    )
    driver.execute_script("arguments[0].scrollIntoView(true);", boton)
    driver.execute_script("arguments[0].click();", boton)
    wait.until(EC.invisibility_of_element_located((By.ID, "statusDialog")))
    time.sleep(1)

# ── Extraer detalle de la página interior ───────────────────
def extraer_detalle(driver):
    """
    Extrae todos los campos del panel de detalle usando FIELD_MAP
    para normalizar etiquetas → nombres de columna.
    """
    detalle = {}

    # Bloque principal: li con sub-titulo-2 + detalle-sp
    items = driver.find_elements(By.CSS_SELECTOR, "div.col-sm-12.cuadro-seccion li")
    for item in items:
        try:
            etiq = item.find_element(By.CSS_SELECTOR, "span.sub-titulo-2").text.strip()
            val  = item.find_element(By.CSS_SELECTOR, "span.detalle-sp").text.strip()
            detalle[_mapear(etiq)] = val
        except Exception:
            continue

    # Bloque lateral (N° convocatoria, otros datos sueltos)
    laterales = driver.find_elements(
        By.CSS_SELECTOR, "div.col-sm-12.cuadro-seccion-lat li"
    )
    for item in laterales:
        try:
            etiq = item.find_element(By.CSS_SELECTOR, "span.sub-titulo-2").text.strip()
            val  = item.find_element(By.CSS_SELECTOR, "span.detalle-sp").text.strip()
            detalle[_mapear(etiq)] = val
        except Exception:
            continue

    # Texto libre "DETALLE:"
    try:
        detalle_span = driver.find_element(
            By.XPATH,
            "//span[@class='sub-titulo' and contains(text(),'DETALLE:')]"
            "/following-sibling::span"
        )
        detalle["detalle"] = detalle_span.text.strip()
    except Exception:
        pass

    # N° convocatoria alternativo si aún no se capturó
    if "numero_convocatoria" not in detalle:
        try:
            id_span = driver.find_element(
                By.CSS_SELECTOR,
                "div.cuadro-seccion-lat span.sub-titulo-2"
            )
            detalle["id_convocatoria"] = id_span.text.strip()  # guarda "N° 769250" tal cual
        except Exception:
            pass


    return detalle

# ── Volver a la lista ────────────────────────────────────────
def volver_a_lista(driver):
    wait = WebDriverWait(driver, 5)
    boton = wait.until(
        EC.element_to_be_clickable((By.ID, "frmRegresar:j_idt30"))
    )
    driver.execute_script("arguments[0].scrollIntoView(true);", boton)
    driver.execute_script("arguments[0].click();", boton)
    wait.until(EC.invisibility_of_element_located((By.ID, "statusDialog")))
    time.sleep(1)

def recorrer_paginas(driver):
    resultados = []
    wait = WebDriverWait(driver, 5)
    pagina = 0

    while pagina<2:  # limitar a 2 páginas para pruebas
        print(f"Procesando página {pagina + 1}...")

        ofertas = extraer_convocatorias(driver)

        for i in range(len(ofertas)):
            entrar_ver_mas(driver, i)

            detalle = extraer_detalle(driver)

            
            ofertas[i].update(detalle)
            resultados.append(ofertas[i])
            print(f"✔ Registro {len(resultados)}:")
            print(ofertas[i])
            print("-" * 50)
            volver_a_lista(driver)
            # 👇 CLAVE: esperar que la lista esté de nuevo
            wait.until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "div.col-sm-12.cuadro-vacantes")
                    )
                )

        # intentar ir a la siguiente página
        try:
            boton_sig = wait.until(
                EC.presence_of_element_located((By.ID, "frmLstOfertsLabo:j_idt56"))
            )

            if "ui-state-disabled" in boton_sig.get_attribute("class"):
                print("No hay más páginas.")
                break

            driver.execute_script("arguments[0].click();", boton_sig)
            time.sleep(2)

            pagina += 1

        except:
            print("No se pudo encontrar el botón siguiente.")
            break

    return resultados


