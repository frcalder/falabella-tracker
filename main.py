"""
Aplicación principal para extraer y visualizar datos de movimientos de tarjeta de Banco Falabella.
"""
import sys
import logging
import argparse
import subprocess
from scraper.bank_scraper import main as run_scraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_dashboard():
    """Inicia el dashboard de Streamlit."""
    try:
        subprocess.Popen(
            [sys.executable, "-m", "streamlit", "run", "dashboard/visualizer.py"],
        ).wait()
    except KeyboardInterrupt:
        print("\nDashboard detenido.")


def main():
    parser = argparse.ArgumentParser(description='Scraper de movimientos de tarjeta Falabella')
    parser.add_argument('--mode', default='scraper', choices=['scraper', 'dashboard'])
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--headless', action='store_true')
    parser.add_argument('--limit', type=int, default=0, help='Máx movimientos por página (0=todos)')
    parser.add_argument('--backfill-periodo', default='',
                        help="Fuerza el backfill de un período cerrado desde 'Movimientos "
                             "facturados' ('YYYY-MM'). Sin este flag, el backfill del período "
                             "recién cerrado corre solo una vez, cuando el banco publica el detalle")
    parser.add_argument('--backfill-dry-run', action='store_true',
                        help='Lista las filas que faltan sin escribir en la DB')
    args = parser.parse_args()

    if args.mode == 'scraper':
        run_scraper(debug_mode=args.debug, headless=args.headless, max_per_page=args.limit,
                    backfill_periodo=args.backfill_periodo,
                    backfill_dry_run=args.backfill_dry_run)
    else:
        run_dashboard()


if __name__ == "__main__":
    main()
