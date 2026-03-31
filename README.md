# Falabella Tracker

Herramienta personal para extraer y visualizar movimientos de tarjeta de crédito del Banco Falabella (Chile). Usa Playwright para scrapear transacciones, las almacena en Supabase PostgreSQL y las visualiza en un dashboard Streamlit con clasificación, presupuestos y análisis de gastos.

> **Nota:** Esta herramienta está construida sobre la interfaz web del Banco Falabella Chile (Angular SPA con Shadow DOM). Puede dejar de funcionar si el banco modifica su frontend.

## Funcionalidades

- Scraping diario automatizado de transacciones vía GitHub Actions
- Manejo de movimientos pendientes, confirmados, cuotas y pagos divididos (splits)
- Clasificación de transacciones por categoría con sugerencias basadas en comercio
- Seguimiento de presupuesto mensual por período de facturación
- Análisis de gastos con comparación proporcional contra el período anterior
- Trigger manual del scraper desde el dashboard
- Backups diarios de la base de datos en el repositorio

## Stack

| Capa | Tecnología |
|---|---|
| Scraper | Python + Playwright (async) |
| Base de datos | Supabase PostgreSQL |
| Dashboard | Streamlit (multi-página) |
| Automatización | GitHub Actions |

## Arquitectura

El sitio del banco es una SPA Angular que usa **Shadow DOM** — los selectores CSS estándar no alcanzan los componentes internos. Toda la interacción con el DOM se realiza vía `page.evaluate()` con JavaScript que recorre los shadow roots de forma recursiva.

```
GitHub Actions (diario)
    └── scraper/bank_scraper.py  →  Supabase PostgreSQL
                                         ↑
                              dashboard/visualizer.py (Streamlit)
```

---

## Primeros pasos

### Requisitos previos

- Python 3.11+
- [Pipenv](https://pipenv.pypa.io/)
- Un proyecto en [Supabase](https://supabase.com/) (el tier gratuito funciona) — **requerido tanto para correr local como en la nube; no hay opción de base de datos local**
- Una cuenta en Banco Falabella Chile

### 1. Fork y clonar el repositorio

Primero haz fork de [https://github.com/frcalder/falabella-tracker](https://github.com/frcalder/falabella-tracker) en tu cuenta de GitHub. Para hacer el fork, abre el enlace y haz clic en el botón **Fork** que aparece arriba a la derecha — esto crea una copia del repositorio en tu cuenta. Esto te permite tener tu propia copia del repo donde GitHub Actions correrá el scraper y Streamlit Cloud desplegará el dashboard.

Luego clona **tu fork**:

```bash
git clone https://github.com/TU_USUARIO/falabella-tracker.git
cd falabella-tracker
pip install pipenv
pipenv install
pipenv run playwright install chromium
```

### 2. Crear un proyecto en Supabase

Ve a [supabase.com](https://supabase.com/), crea un nuevo proyecto y obtén el string de conexión desde **Project Settings → Database → Connection string → Session mode** (usa el Session Pooler para compatibilidad IPv4):

```
postgresql://postgres.[ref]:[password]@aws-0-us-west-2.pooler.supabase.com:5432/postgres
```

> El tier gratuito es suficiente. Supabase es la única opción de base de datos — el proyecto no soporta SQLite ni bases de datos locales.

### 3. Configurar credenciales

Copia `env.example` a `.env` y completa con tus valores:

```bash
cp env.example .env
```

```env
FALABELLA_USER=tu_rut
FALABELLA_PASSWORD=tu_clave
DATABASE_URL=postgresql://postgres.[ref]:[password]@aws-0-us-west-2.pooler.supabase.com:5432/postgres
```

### 4. Inicializar la base de datos

Aplica el schema en tu proyecto de Supabase:

```bash
pipenv run python -c "from analytics.db import get_connection, init_db; conn = get_connection(); init_db(conn); conn.close()"
```

Alternativamente, ejecuta `analytics/schema.sql` directamente desde el editor SQL de Supabase.

### 5. Ejecutar el scraper

```bash
# Ejecución completa (abre ventana del navegador)
pipenv run python main.py --mode scraper

# Modo headless
pipenv run python main.py --mode scraper --headless

# Limitar filas por página (útil para pruebas)
pipenv run python main.py --mode scraper --limit 1

# Modo debug — guarda screenshots en debug/
pipenv run python main.py --mode scraper --debug
```

### 6. Iniciar el dashboard

```bash
pipenv run python main.py --mode dashboard
# o directamente:
pipenv run streamlit run dashboard/visualizer.py
```

Abre [http://localhost:8501](http://localhost:8501) en tu navegador.

---

## Flujo de uso

El orden lógico para comenzar a operar es:

1. **Correr el scraper** — extrae los movimientos del banco y los guarda en la base de datos. Sin esto, el dashboard no tiene datos.
2. **Crear categorías y presupuesto** (página Presupuesto) — define las categorías que usarás y asigna montos por período. La base de datos parte vacía, sin categorías predefinidas.
3. **Clasificar movimientos** (página Clasificación) — asigna cada movimiento a una categoría. Si un gasto se divide entre varias categorías (ej. compra en supermercado con ítem de farmacia), usa splits para repartir el monto.
4. **Revisar el análisis** (página Análisis) — una vez que los movimientos estén clasificados y haya presupuesto configurado, el análisis muestra el progreso real vs. presupuesto y la comparación contra el período anterior.

---

## Páginas del dashboard

| Página | Descripción |
|---|---|
| **Clasificación** | Clasifica transacciones por categoría. Sugerencias automáticas basadas en el comercio. Permite dividir un movimiento entre varias categorías con montos parciales. |
| **Presupuesto** | Define presupuesto mensual por categoría y período de facturación. |
| **Análisis** | Progreso de gastos vs. presupuesto, comparación proporcional contra el período anterior, desglose por categoría con reclasificación inline y tendencia histórica. |
| **Scraper** | Historial de ejecuciones y trigger manual vía GitHub Actions. |

---

## Despliegue completo en la nube

Para ejecutar el stack completo de forma automatizada necesitas configurar tres servicios.

### 1. Supabase (base de datos)

Si ya seguiste los pasos de configuración local, **ya tienes esto listo** — es el mismo proyecto Supabase y el mismo `DATABASE_URL`. Solo necesitas tener el schema inicializado (paso 4 de arriba).

### 2. GitHub Actions (scraper automatizado + backups)

Los workflows en `.github/workflows/` se ejecutan automáticamente una vez que configures los secrets en tu fork.

Ve a **Settings → Secrets and variables → Actions** y agrega:

| Secret | Valor |
|---|---|
| `FALABELLA_USER` | Tu RUT |
| `FALABELLA_PASSWORD` | Tu clave de Falabella |
| `DATABASE_URL` | Tu string de conexión a Supabase |

Workflows incluidos:
- `scraper.yml` — corre diariamente a las 08:00 hora Chile, también ejecutable manualmente
- `backup.yml` — corre diariamente a las 09:00 hora Chile, commitea un backup JSON en `backups/`

> Los secrets nunca se exponen en los logs ni a forks. Cada persona que haga fork necesita configurar sus propios secrets.

### 3. Streamlit Cloud (dashboard)

1. Conecta tu fork en [share.streamlit.io](https://share.streamlit.io/)
2. Configura el archivo principal como `dashboard/visualizer.py`
3. Agrega los secrets en **App settings → Secrets**:

```toml
DATABASE_URL = "postgresql://..."
GITHUB_TOKEN = "ghp_..."   # Personal Access Token con permisos repo + actions:write
```

El `GITHUB_TOKEN` es necesario para disparar el scraper manualmente desde el dashboard.

---

## Variables de entorno

| Variable | Requerida | Descripción |
|---|---|---|
| `FALABELLA_USER` | Sí | RUT (sin puntos, con guión) |
| `FALABELLA_PASSWORD` | Sí | Clave de banca en línea Falabella |
| `DATABASE_URL` | Sí | String de conexión PostgreSQL |

---

## Schema de base de datos

Ver [`analytics/schema.sql`](analytics/schema.sql) para el schema completo. Tablas principales:

| Tabla | Descripción |
|---|---|
| `movimientos` | Todas las transacciones scrapeadas |
| `categorias` | Categorías definidas por el usuario |
| `clasificaciones` | Mapeo transacción → categoría |
| `splits` | Asignaciones parciales entre múltiples categorías |
| `presupuestos` | Presupuesto mensual por categoría y período |
| `scraper_runs` | Log de ejecución de cada corrida del scraper |

---

## Períodos de facturación

El período de facturación se obtiene directamente del sitio del banco — el scraper extrae el texto "Próxima facturación DD/MM/YYYY" de la página de movimientos y lo asigna a cada transacción. No requiere configuración manual. Todas las páginas (Clasificación, Presupuesto, Análisis) usan esta misma clave de período para referencias cruzadas.

---

## Aviso legal

Este es un proyecto personal sin ninguna afiliación con Banco Falabella. Úsalo bajo tu propio riesgo. El scraping automatizado puede ir en contra de los términos de servicio del banco.
